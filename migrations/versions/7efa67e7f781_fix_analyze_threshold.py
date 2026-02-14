"""fix analyze threshold

Revision ID: 7efa67e7f781
Revises: 6edcd3e80714
Create Date: 2026-02-14 13:50:58.488070

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '7efa67e7f781'
down_revision = '6edcd3e80714'
branch_labels = None
depends_on = None


def set_storage_params(table, **kwargs):
    storage_params = ', '.join(
        f"{param} = {str(value).lower()}" for param, value in kwargs.items()
    )
    op.execute(f"ALTER TABLE {table} SET ({storage_params})")


def reset_storage_params(table, param_names):
    op.execute(f"ALTER TABLE {table} RESET ({', '.join(param_names)})")


def upgrade():
    # Buffer tables:
    reset_storage_params(
        'pending_log_entry',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'pending_ledger_update',
        [
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'configure_account_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepare_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalize_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'updated_ledger_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'updated_policy_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'updated_flags_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'rejected_config_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )


def downgrade():
    # Buffer tables:
    set_storage_params(
        'pending_log_entry',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'pending_ledger_update',
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'configure_account_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepare_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalize_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'updated_ledger_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'updated_policy_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'updated_flags_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'rejected_config_signal',
        autovacuum_analyze_threshold=2000000000,
    )
