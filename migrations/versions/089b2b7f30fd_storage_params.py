"""storage params

Revision ID: 089b2b7f30fd
Revises: d98019373169
Create Date: 2025-12-14 13:04:39.851709

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '089b2b7f30fd'
down_revision = 'd98019373169'
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
    # TODO: defer loading?
    op.execute("ALTER TABLE account_data ALTER COLUMN config_data SET STORAGE EXTERNAL")
    op.execute("ALTER TABLE running_transfer ALTER COLUMN transfer_note SET STORAGE EXTERNAL")
    op.execute("ALTER TABLE committed_transfer ALTER COLUMN transfer_note SET STORAGE EXTERNAL")

    # Related to creditors:
    set_storage_params(
        'creditor',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.08,
    )
    set_storage_params(
        'pin_info',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'usage_stats',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'log_entry',
        fillfactor=100,
        autovacuum_vacuum_threshold=100000,
        autovacuum_vacuum_scale_factor=0.0005,
        autovacuum_vacuum_insert_threshold=100000,
        autovacuum_vacuum_insert_scale_factor=0.0005,
    )

    # Related to accounts:
    set_storage_params(
        'account',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'account_data',
        toast_tuple_target=600,
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'account_knowledge',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'account_exchange',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'account_display',
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'ledger_entry',
        fillfactor=100,
        autovacuum_vacuum_threshold=100000,
        autovacuum_vacuum_scale_factor=0.0005,
        autovacuum_vacuum_insert_threshold=100000,
        autovacuum_vacuum_insert_scale_factor=0.0005,
    )

    # Related to transfers:
    set_storage_params(
        'running_transfer',
        fillfactor=100,
        autovacuum_vacuum_threshold=10000,
        autovacuum_vacuum_scale_factor=0.00002,
        autovacuum_vacuum_insert_threshold=10000,
        autovacuum_vacuum_insert_scale_factor=0.00002,
    )
    set_storage_params(
        'committed_transfer',
        fillfactor=100,
        autovacuum_vacuum_threshold=100000,
        autovacuum_vacuum_scale_factor=0.0005,
        autovacuum_vacuum_insert_threshold=100000,
        autovacuum_vacuum_insert_scale_factor=0.0005,
    )

    # Buffer tables:
    set_storage_params(
        'pending_log_entry',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'pending_ledger_update',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'configure_account_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepare_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalize_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'updated_ledger_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'updated_policy_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'updated_flags_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'rejected_config_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )


def downgrade():
    op.execute("ALTER TABLE account_data ALTER COLUMN config_data SET STORAGE DEFAULT")
    op.execute("ALTER TABLE running_transfer ALTER COLUMN transfer_note SET STORAGE DEFAULT")
    op.execute("ALTER TABLE committed_transfer ALTER COLUMN transfer_note SET STORAGE DEFAULT")

    # Related to creditors:
    reset_storage_params(
        'creditor',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'pin_info',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'usage_stats',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'log_entry',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )

    # Related to accounts:
    reset_storage_params(
        'account',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'account_data',
        [
            'toast_tuple_target',
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'account_knowledge',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'account_exchange',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'account_display',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'ledger_entry',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )

    # Related to transfers:
    reset_storage_params(
        'running_transfer',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'committed_transfer',
        [
            'fillfactor',
            'autovacuum_vacuum_threshold',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )

    # Buffer tables:
    reset_storage_params(
        'pending_log_entry',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'pending_ledger_update',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'configure_account_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepare_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalize_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'updated_ledger_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'updated_policy_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'updated_flags_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'rejected_config_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
