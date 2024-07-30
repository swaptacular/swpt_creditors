"""Create stored procedures

Revision ID: 54696121695a
Revises: d5396ac38ffe
Create Date: 2024-07-29 18:49:17.251175

"""
from alembic import op
import sqlalchemy as sa

from swpt_creditors.migration_helpers import ReplaceableObject

# revision identifiers, used by Alembic.
revision = '54696121695a'
down_revision = 'd5396ac38ffe'
branch_labels = None
depends_on = None

is_account_creation_allowed_sp = ReplaceableObject(
    "is_account_creation_allowed(cid BIGINT, max_accounts SMALLINT, max_reconfigs SMALLINT)",
    """
    RETURNS boolean AS $$
    DECLARE
      current_epoch INTEGER = floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600.0);
      allowed BOOLEAN;
    BEGIN
      SELECT FALSE INTO allowed
      FROM usage_stats
      WHERE
        creditor_id=cid
        AND (
          accounts_count >= max_accounts
          OR CASE WHEN current_epoch < reconfigs_reset_at THEN reconfigs_count ELSE 0 END >= max_reconfigs
        );

      RETURN COALESCE(allowed, max_accounts > 0 AND max_reconfigs > 0);
    END;
    $$ LANGUAGE plpgsql;
    """
)

is_account_reconfig_allowed_sp = ReplaceableObject(
    "is_account_reconfig_allowed(cid BIGINT, max_reconfigs SMALLINT)",
    """
    RETURNS boolean AS $$
    DECLARE
      current_epoch INTEGER = floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600.0);
      allowed BOOLEAN;
    BEGIN
      SELECT FALSE INTO allowed
      FROM usage_stats
      WHERE
        creditor_id=cid
        AND CASE WHEN current_epoch < reconfigs_reset_at THEN reconfigs_count ELSE 0 END >= max_reconfigs;

      RETURN COALESCE(allowed, max_reconfigs > 0);
    END;
    $$ LANGUAGE plpgsql;
    """
)

is_transfer_creation_allowed_sp = ReplaceableObject(
    "is_transfer_creation_allowed(cid BIGINT, max_transfers SMALLINT, max_initiations SMALLINT)",
    """
    RETURNS boolean AS $$
    DECLARE
      current_epoch INTEGER = floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600.0);
      allowed BOOLEAN;
    BEGIN
      SELECT FALSE INTO allowed
      FROM usage_stats
      WHERE
        creditor_id=cid
        AND (
          transfers_count >= max_transfers
          OR CASE WHEN current_epoch < initiations_reset_at THEN initiations_count ELSE 0 END >= max_initiations
        );

      RETURN COALESCE(allowed, max_transfers > 0 AND max_initiations > 0);
    END;
    $$ LANGUAGE plpgsql;
    """
)

register_account_creation_sp = ReplaceableObject(
    "register_account_creation(cid BIGINT, reconfig_clear_hours INTEGER)",
    """
    RETURNS void AS $$
    DECLARE
      current_epoch INTEGER = floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600.0);
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET
        accounts_count = LEAST(accounts_count + 1, 32767),
        reconfigs_count = (
          CASE WHEN current_epoch < reconfigs_reset_at
          THEN LEAST(reconfigs_count + 1, 32767)
          ELSE 1 END
        ),
        reconfigs_reset_at = (
          CASE WHEN current_epoch < reconfigs_reset_at
          THEN reconfigs_reset_at
          ELSE current_epoch + reconfig_clear_hours END
        )
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, accounts_count, reconfigs_count, reconfigs_reset_at)
          VALUES (cid, 1, 1, current_epoch + reconfig_clear_hours);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

register_account_reconfig_sp = ReplaceableObject(
    "register_account_reconfig(cid BIGINT, reconfig_clear_hours INTEGER)",
    """
    RETURNS void AS $$
    DECLARE
      current_epoch INTEGER = floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600.0);
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET
        reconfigs_count = (
          CASE WHEN current_epoch < reconfigs_reset_at
          THEN LEAST(reconfigs_count + 1, 32767)
          ELSE 1 END
        ),
        reconfigs_reset_at = (
          CASE WHEN current_epoch < reconfigs_reset_at
          THEN reconfigs_reset_at
          ELSE current_epoch + reconfig_clear_hours END
        )
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, reconfigs_count, reconfigs_reset_at)
          VALUES (cid, 1, current_epoch + reconfig_clear_hours);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

register_transfer_creation_sp = ReplaceableObject(
    "register_transfer_creation(cid BIGINT, initiations_clear_hours INTEGER)",
    """
    RETURNS void AS $$
    DECLARE
      current_epoch INTEGER = floor(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) / 3600.0);
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET
        transfers_count = LEAST(transfers_count + 1, 32767),
        initiations_count = (
          CASE WHEN current_epoch < initiations_reset_at
          THEN LEAST(initiations_count + 1, 32767)
          ELSE 1 END
        ),
        initiations_reset_at = (
          CASE WHEN current_epoch < initiations_reset_at
          THEN initiations_reset_at
          ELSE current_epoch + initiations_clear_hours END
        )
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, transfers_count, initiations_count, initiations_reset_at)
          VALUES (cid, 1, 1, current_epoch + initiations_clear_hours);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

increment_account_number_sp = ReplaceableObject(
    "increment_account_number(cid BIGINT)",
    """
    RETURNS void AS $$
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET accounts_count = LEAST(accounts_count + 1, 32767)
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, accounts_count) VALUES (cid, 1);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

decrement_account_number_sp = ReplaceableObject(
    "decrement_account_number(cid BIGINT)",
    """
    RETURNS void AS $$
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET accounts_count = GREATEST(accounts_count - 1, -32768)
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, accounts_count) VALUES (cid, -1);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

increment_transfer_number_sp = ReplaceableObject(
    "increment_transfer_number(cid BIGINT)",
    """
    RETURNS void AS $$
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET transfers_count = LEAST(transfers_count + 1, 32767)
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, transfers_count) VALUES (cid, 1);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)

decrement_transfer_number_sp = ReplaceableObject(
    "decrement_transfer_number(cid BIGINT)",
    """
    RETURNS void AS $$
    BEGIN
      SET LOCAL synchronous_commit = off;

      UPDATE usage_stats
      SET transfers_count = GREATEST(transfers_count - 1, -32768)
      WHERE creditor_id=cid;

      IF NOT FOUND THEN
        BEGIN
          INSERT INTO usage_stats (creditor_id, transfers_count) VALUES (cid, -1);
        EXCEPTION
          WHEN unique_violation OR foreign_key_violation THEN
            RETURN;
        END;
      END IF;
    END;
    $$ LANGUAGE plpgsql;
    """
)


def upgrade():
    op.create_sp(is_account_creation_allowed_sp)
    op.create_sp(is_account_reconfig_allowed_sp)
    op.create_sp(is_transfer_creation_allowed_sp)
    op.create_sp(register_account_creation_sp)
    op.create_sp(register_account_reconfig_sp)
    op.create_sp(register_transfer_creation_sp)
    op.create_sp(increment_account_number_sp)
    op.create_sp(decrement_account_number_sp)
    op.create_sp(increment_transfer_number_sp)
    op.create_sp(decrement_transfer_number_sp)


def downgrade():
    op.drop_sp(is_account_creation_allowed_sp)
    op.drop_sp(is_account_reconfig_allowed_sp)
    op.drop_sp(is_transfer_creation_allowed_sp)
    op.drop_sp(register_account_creation_sp)
    op.drop_sp(register_account_reconfig_sp)
    op.drop_sp(register_transfer_creation_sp)
    op.drop_sp(increment_account_number_sp)
    op.drop_sp(decrement_account_number_sp)
    op.drop_sp(increment_transfer_number_sp)
    op.drop_sp(decrement_transfer_number_sp)
