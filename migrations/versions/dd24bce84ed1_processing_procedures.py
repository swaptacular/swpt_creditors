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
      account_id VARCHAR,
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
      previous_principal NUMERIC(24);
      ledger_principal BIGINT;
      correction_amount NUMERIC(24);
      safe_amount BIGINT;
    BEGIN
      made_correcting_ledger_entry := FALSE;

      previous_principal := (
        principal::NUMERIC(24) - acquired_amount::NUMERIC(24)
      );
      IF previous_principal < -9223372036854775808::NUMERIC(24)
         OR previous_principal > 9223372036854775807::NUMERIC(24)
           THEN
         RETURN;  -- Normally, this should not happen.
      END IF;

      -- We will make correcting transfers until `ledger_principal` becomes
      -- equal to `previous_principal`.
      ledger_principal := data.ledger_principal;
      correction_amount := previous_principal - ledger_principal::NUMERIC(24);

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
    " max_delay INTERVAL"
    ")",
    """
    RETURNS void AS $$
    DECLARE
      data account_ledger_data%ROWTYPE;
      previous_transfer_number BIGINT;
      transfer_number BIGINT;
      acquired_amount BIGINT;
      principal BIGINT;
      committed_at TIMESTAMP WITH TIME ZONE;
      ulr update_ledger_result%ROWTYPE;
      log_entry pending_log_entry_result%ROWTYPE;
      committed_at_cutoff TIMESTAMP WITH TIME ZONE = (
        CURRENT_TIMESTAMP - max_delay
      );
    BEGIN
      PERFORM
      FROM pending_ledger_update
      WHERE creditor_id = cid AND debtor_id = did
      FOR UPDATE;

      IF FOUND THEN
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
          NULL,
          ad.last_transfer_number,
          ad.last_transfer_committed_at
        INTO STRICT data
        FROM account_data ad
        WHERE ad.creditor_id = cid AND ad.debtor_id = did
        FOR NO KEY UPDATE;

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

        LOOP
          IF previous_transfer_number != data.ledger_last_transfer_number
             AND committed_at >= committed_at_cutoff
                THEN
              -- We are missing a transfer.
              data.ledger_pending_transfer_ts = committed_at;
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
        END LOOP;

        IF (
              data.ledger_pending_transfer_ts IS NULL
              AND data.last_transfer_number > data.ledger_last_transfer_number
              AND data.last_transfer_committed_at < committed_at_cutoff
            ) THEN
          -- We are missing the latest transfers, and we have given up hope
          -- to receive them. Here we create a fake "catch-up" ledger entry.
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

        IF log_entry IS NOT NULL THEN
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

          LOOP
            EXIT WHEN (
              nextval('object_update_id_seq') >= data.ledger_latest_update_id
            );
          END LOOP;
        END IF;

        UPDATE account_data
        SET
          ledger_principal = data.ledger_principal,
          ledger_last_entry_id = data.ledger_last_entry_id,
          ledger_last_transfer_number = data.ledger_last_transfer_number,
          ledger_latest_update_id = data.ledger_latest_update_id,
          ledger_latest_update_ts = data.ledger_latest_update_ts,
          ledger_pending_transfer_ts = data.ledger_pending_transfer_ts
        WHERE creditor_id = cid AND debtor_id = did;

        DELETE FROM pending_ledger_update
        WHERE creditor_id = cid AND debtor_id = did;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

process_pending_log_entries_sp = ReplaceableObject(
    "process_pending_log_entries(cid BIGINT)",
    """
    RETURNS void AS $$
    DECLARE
      cr creditor%ROWTYPE;
      entry pending_log_entry%ROWTYPE;
    BEGIN
      SELECT * INTO cr
      FROM creditor
      WHERE creditor_id = cid
      FOR NO KEY UPDATE;

      FOR entry IN
        SELECT *
        FROM pending_log_entry
        WHERE creditor_id = cid
        FOR UPDATE SKIP LOCKED

      LOOP
        IF cr.creditor_id IS NOT NULL THEN
          cr.last_log_entry_id := cr.last_log_entry_id + 1;
          INSERT INTO log_entry (
            creditor_id, entry_id, object_type,
            object_uri, object_update_id, added_at,
            is_deleted, data, object_type_hint,
            debtor_id, creation_date, transfer_number,
            transfer_uuid, data_principal,
            data_next_entry_id, data_finalized_at,
            data_error_code
          )
          VALUES (
            cr.creditor_id, cr.last_log_entry_id, entry.object_type,
            entry.object_uri, entry.object_update_id, entry.added_at,
            entry.is_deleted, entry.data, entry.object_type_hint,
            entry.debtor_id, entry.creation_date, entry.transfer_number,
            entry.transfer_uuid, entry.data_principal,
            entry.data_next_entry_id, entry.data_finalized_at,
            entry.data_error_code
          );

          IF  (
                (entry.object_type IS NULL AND entry.object_type_hint = 1)
                OR entry.object_type = 'Transfer'
              )
              AND (
                entry.object_update_id IS NULL
                OR entry.object_update_id = 1
                OR entry.is_deleted
              ) THEN
            -- A transfer object has been created or deleted, and therefore
            -- we need to insert a "TransfersList" update log entry.
            cr.last_log_entry_id := cr.last_log_entry_id + 1;
            cr.transfers_list_latest_update_ts := entry.added_at;
            cr.transfers_list_latest_update_id := (
              cr.transfers_list_latest_update_id + 1
            );
            INSERT INTO log_entry (
              creditor_id, entry_id,
              added_at,
              object_update_id,
              object_type_hint
            )
            VALUES (
              cr.creditor_id, cr.last_log_entry_id,
              cr.transfers_list_latest_update_ts,
              cr.transfers_list_latest_update_id,
              2
            );
          END IF;
        END IF;

        DELETE FROM pending_log_entry
        WHERE
          creditor_id = entry.creditor_id
          AND pending_entry_id = entry.pending_entry_id;
      END LOOP;

      IF cr.creditor_id IS NOT NULL THEN
        UPDATE creditor
        SET
          last_log_entry_id = cr.last_log_entry_id,
          transfers_list_latest_update_id = cr.transfers_list_latest_update_id,
          transfers_list_latest_update_ts = cr.transfers_list_latest_update_ts
        WHERE creditor_id = cid;
      END IF;
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
    op.create_sp(process_pending_log_entries_sp)

    with op.batch_alter_table('pending_log_entry', schema=None) as batch_op:
        batch_op.drop_constraint('pending_log_entry_creditor_id_fkey', type_='foreignkey')


def downgrade():
    op.execute("DELETE FROM pending_log_entry WHERE creditor_id NOT IN (SELECT creditor_id FROM creditor)")

    with op.batch_alter_table('pending_log_entry', schema=None) as batch_op:
        batch_op.create_foreign_key('pending_log_entry_creditor_id_fkey', 'creditor', ['creditor_id'], ['creditor_id'], ondelete='CASCADE')

    op.drop_sp(process_pending_log_entries_sp)
    op.drop_sp(process_pending_ledger_update_sp)
    op.drop_sp(update_ledger_sp)
    op.drop_sp(make_correcting_ledger_entry_if_necessary_sp)
    op.drop_sp(contain_principal_overflow_sp)
    op.drop_type(make_correcting_ledger_entry_result_type)
    op.drop_type(update_ledger_result_type)
    op.drop_type(pending_log_entry_result_type)
    op.drop_type(account_ledger_data_type)
