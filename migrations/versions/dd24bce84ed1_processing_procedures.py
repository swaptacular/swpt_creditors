"""processing procedures

Revision ID: dd24bce84ed1
Revises: 54696121695a
Create Date: 2024-08-11 12:48:12.383040

"""
from alembic import op
import sqlalchemy as sa

from swpt_creditors.migration_helpers import ReplaceableObject

# revision identifiers, used by Alembic.
revision = 'dd24bce84ed1'
down_revision = '54696121695a'
branch_labels = None
depends_on = None

account_ledger_data_type = ReplaceableObject(
    "account_ledger_data",
    """
    AS (
      creditor_id BIGINT,
      debtor_id BIGINT,
      creation_date DATE,
      principal BIGINT,
      account_id TEXT,
      ledger_principal BIGINT,
      ledger_last_entry_id BIGINT,
      ledger_last_transfer_number BIGINT,
      ledger_latest_update_id BIGINT,
      ledger_latest_update_ts TIMESTAMP WITH TIME ZONE,
      ledger_pending_transfer_ts TIMESTAMP WITH TIME ZONE,
      last_transfer_number BIGINT,
      last_transfer_committed_at TIMESTAMP WITH TIME ZONE
    )
    """
)

pending_log_entry_result_type = ReplaceableObject(
    "pending_log_entry_result",
    """
    AS (
      creditor_id BIGINT,
      added_at TIMESTAMP WITH TIME ZONE,
      object_type_hint SMALLINT,
      debtor_id BIGINT,
      object_update_id BIGINT,
      data_principal BIGINT,
      data_next_entry_id BIGINT
    )
    """
)

update_ledger_result_type = ReplaceableObject(
    "update_ledger_result",
    """
    AS (
      data account_ledger_data,
      log_entry pending_log_entry_result
    )
    """
)

make_correcting_ledger_entry_result_type = ReplaceableObject(
    "make_correcting_ledger_entry_result",
    """
    AS (
      data account_ledger_data,
      made_correcting_ledger_entry BOOLEAN
    )
    """
)

contain_principal_overflow_sp = ReplaceableObject(
    "contain_principal_overflow(value NUMERIC(24))",
    """
    RETURNS BIGINT AS $$
    DECLARE
      min_value value%TYPE = -9223372036854775807;
      max_value value%TYPE = 9223372036854775807;
    BEGIN
      IF value < min_value THEN
        RETURN min_value;
      ELSIF value > max_value THEN
        RETURN max_value;
      ELSE
        RETURN value;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

make_correcting_ledger_entry_if_necessary_sp = ReplaceableObject(
    "make_correcting_ledger_entry_if_necessary("
    " INOUT data account_ledger_data,"
    " acquired_amount BIGINT,"
    " principal BIGINT,"
    " current_ts TIMESTAMP WITH TIME ZONE,"
    " OUT made_correcting_ledger_entry BOOLEAN"
    ")",
    """
    AS $$
    DECLARE
      previous_principal BIGINT;
      ledger_principal BIGINT;
      correction_amount NUMERIC(24);
      safe_amount BIGINT;
    BEGIN
      made_correcting_ledger_entry := FALSE;

      BEGIN
        previous_principal := principal - acquired_amount;
      EXCEPTION
        WHEN numeric_value_out_of_range THEN
          RETURN;
      END;

      ledger_principal := data.ledger_principal;

      -- We will make correcting transfers until `ledger_principal` becomes
      -- equal to `previous_principal`.
      correction_amount := (
        previous_principal::NUMERIC(24) - ledger_principal::NUMERIC(24)
      );

      LOOP
        EXIT WHEN correction_amount = 0;

        safe_amount := contain_principal_overflow(correction_amount);
        correction_amount = correction_amount - safe_amount::NUMERIC(24);
        ledger_principal = ledger_principal + safe_amount;
        data.ledger_last_entry_id := data.ledger_last_entry_id + 1;

        INSERT INTO ledger_entry (
          creditor_id, debtor_id, entry_id,
          acquired_amount, principal, added_at
        )
        VALUES (
          data.creditor_id, data.debtor_id, data.ledger_last_entry_id,
          safe_amount, ledger_principal, current_ts
        );

        made_correcting_ledger_entry := TRUE;
      END LOOP;
    END;
    $$ LANGUAGE plpgsql;
    """
)

update_ledger_sp = ReplaceableObject(
    "update_ledger("
    " INOUT data account_ledger_data,"
    " transfer_number BIGINT,"
    " acquired_amount BIGINT,"
    " principal BIGINT,"
    " current_ts TIMESTAMP WITH TIME ZONE,"
    " OUT log_entry pending_log_entry_result"
    ")",
    """
    AS $$
    DECLARE
      r make_correcting_ledger_entry_result%ROWTYPE;
      should_return_log_entry BOOLEAN;
    BEGIN
      r := make_correcting_ledger_entry_if_necessary(
        data, acquired_amount, principal, current_ts
      );
      data := r.data;
      should_return_log_entry := r.made_correcting_ledger_entry;

      IF acquired_amount != 0 THEN
        data.ledger_last_entry_id = data.ledger_last_entry_id + 1;

        INSERT INTO ledger_entry (
          creditor_id, debtor_id, entry_id,
          acquired_amount, principal, added_at,
          creation_date, transfer_number
        )
        VALUES (
          data.creditor_id, data.debtor_id, data.ledger_last_entry_id,
          acquired_amount, principal, current_ts,
          data.creation_date, transfer_number
        );

        should_return_log_entry := TRUE;
      END IF;

      data.ledger_principal := principal;
      data.ledger_last_transfer_number := transfer_number;

      IF should_return_log_entry THEN
        data.ledger_latest_update_id := data.ledger_latest_update_id + 1;
        data.ledger_latest_update_ts := current_ts;

        log_entry := ROW(
          data.creditor_id,
          current_ts,
          4,
          data.debtor_id,
          data.ledger_latest_update_id,
          principal,
          data.ledger_last_entry_id + 1
        );
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

process_pending_ledger_update_sp = ReplaceableObject(
    "process_pending_ledger_update("
    " cid BIGINT,"
    " did BIGINT,"
    " max_count INTEGER,"
    " max_delay INTERVAL"
    ")",
    """
    RETURNS BOOLEAN AS $$
    DECLARE
      data account_ledger_data%ROWTYPE;
      committed_at_cutoff TIMESTAMP WITH TIME ZONE;
      n INTEGER;
      previous_transfer_number BIGINT;
      transfer_number BIGINT;
      acquired_amount BIGINT;
      principal BIGINT;
      committed_at TIMESTAMP WITH TIME ZONE;
      is_done BOOLEAN;
      ulr update_ledger_result%ROWTYPE;
      log_entry pending_log_entry_result%ROWTYPE;
    BEGIN
      PERFORM
      FROM pending_ledger_update
      WHERE creditor_id = cid AND debtor_id = did
      FOR UPDATE;

      IF NOT FOUND THEN
        RETURN TRUE;
      END IF;

      SELECT
        ad.creditor_id,
        ad.debtor_id,
        ad.creation_date,
        ad.principal,
        ad.account_id,
        ad.ledger_principal,
        ad.ledger_last_entry_id,
        ad.ledger_last_transfer_number,
        ad.ledger_latest_update_id,
        ad.ledger_latest_update_ts,
        ad.ledger_pending_transfer_ts,
        ad.last_transfer_number,
        ad.last_transfer_committed_at
      INTO data
      FROM account_data ad
      WHERE ad.creditor_id = cid AND ad.debtor_id = did
      FOR UPDATE;

      committed_at_cutoff := CURRENT_TIMESTAMP - max_delay;
      n := 0;

      FOR
        previous_transfer_number,
        transfer_number,
        acquired_amount,
        principal,
        committed_at
      IN
        SELECT
          ct.previous_transfer_number,
          ct.transfer_number,
          ct.acquired_amount,
          ct.principal,
          ct.committed_at
        FROM committed_transfer ct
        WHERE
          ct.creditor_id = data.creditor_id
          AND ct.debtor_id = data.debtor_id
          AND ct.creation_date = data.creation_date
          AND ct.transfer_number > data.ledger_last_transfer_number
        ORDER BY ct.transfer_number
        LIMIT max_count

      LOOP
        IF previous_transfer_number != data.ledger_last_transfer_number
           AND committed_at >= committed_at_cutoff
              THEN
            data.ledger_pending_transfer_ts = committed_at;
            is_done := TRUE;
            EXIT;
        END IF;

        ulr := update_ledger(
          data,
          transfer_number,
          acquired_amount,
          principal,
          CURRENT_TIMESTAMP
        );
        data := ulr.data;
        log_entry := COALESCE(ulr.log_entry, log_entry);
        n := n + 1;
      END LOOP;

      IF is_done IS NULL THEN
        data.ledger_pending_transfer_ts := NULL;
        is_done := n < max_count;
      END IF;

      IF is_done THEN
        IF (
              data.ledger_pending_transfer_ts IS NULL
              AND data.last_transfer_number > data.ledger_last_transfer_number
              AND data.last_transfer_committed_at < committed_at_cutoff
            ) THEN
          ulr := update_ledger(
            data,
            data.last_transfer_number,
            0,
            data.principal,
            CURRENT_TIMESTAMP
          );
          data := ulr.data;
          log_entry := COALESCE(ulr.log_entry, log_entry);
        END IF;

        DELETE FROM pending_ledger_update
        WHERE creditor_id = cid AND debtor_id = did;
      END IF;

      IF log_entry IS NOT NULL THEN
        INSERT INTO updated_ledger_signal (
          creditor_id, debtor_id, update_id,
          account_id, creation_date, principal,
          last_transfer_number, ts,
          inserted_at
        )
        VALUES (
          cid, did, data.ledger_latest_update_id,
          data.account_id, data.creation_date, data.ledger_principal,
          data.ledger_last_transfer_number, CURRENT_TIMESTAMP,
          CURRENT_TIMESTAMP
        );

        INSERT INTO pending_log_entry (
          creditor_id, added_at,
          object_update_id, object_type_hint,
          debtor_id, data_principal,
          data_next_entry_id
        )
        VALUES (
          log_entry.creditor_id, log_entry.added_at,
          log_entry.object_update_id, log_entry.object_type_hint,
          log_entry.debtor_id, log_entry.data_principal,
          log_entry.data_next_entry_id
        );

        PERFORM nextval('object_update_id_seq');
      END IF;

      UPDATE account_data
      SET
        creation_date = data.creation_date,
        principal = data.principal,
        account_id = data.account_id,
        ledger_principal = data.ledger_principal,
        ledger_last_entry_id = data.ledger_last_entry_id,
        ledger_last_transfer_number = data.ledger_last_transfer_number,
        ledger_latest_update_id = data.ledger_latest_update_id,
        ledger_latest_update_ts = data.ledger_latest_update_ts,
        ledger_pending_transfer_ts = data.ledger_pending_transfer_ts,
        last_transfer_number = data.last_transfer_number,
        last_transfer_committed_at = data.last_transfer_committed_at
      WHERE creditor_id = cid AND debtor_id = did;

      RETURN is_done;
    END;
    $$ LANGUAGE plpgsql;
    """
)


def upgrade():
    op.create_type(account_ledger_data_type)
    op.create_type(pending_log_entry_result_type)
    op.create_type(update_ledger_result_type)
    op.create_type(make_correcting_ledger_entry_result_type)
    op.create_sp(contain_principal_overflow_sp)
    op.create_sp(make_correcting_ledger_entry_if_necessary_sp)
    op.create_sp(update_ledger_sp)
    op.create_sp(process_pending_ledger_update_sp)


def downgrade():
    op.drop_sp(process_pending_ledger_update_sp)
    op.drop_sp(update_ledger_sp)
    op.drop_sp(make_correcting_ledger_entry_if_necessary_sp)
    op.drop_sp(contain_principal_overflow_sp)
    op.drop_type(make_correcting_ledger_entry_result_type)
    op.drop_type(update_ledger_result_type)
    op.drop_type(pending_log_entry_result_type)
    op.drop_type(account_ledger_data_type)
