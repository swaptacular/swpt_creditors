"""create pktypes

Revision ID: 6edcd3e80714
Revises: 089b2b7f30fd
Create Date: 2026-02-10 16:37:24.936695

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.inspection import inspect

# revision identifiers, used by Alembic.
revision = '6edcd3e80714'
down_revision = '089b2b7f30fd'
branch_labels = None
depends_on = None


def _pg_type(column_type):
    if column_type.python_type == datetime:
        if column_type.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        else:
            return "TIMESTAMP"

    return str(column_type)


def _pktype_name(model):
    return f"{model.__table__.name}_pktype"


def create_pktype(model):
    mapper = inspect(model)
    type_declaration = ','.join(
        f"{c.key} {_pg_type(c.type)}" for c in mapper.primary_key
    )
    op.execute(
        f"CREATE TYPE {_pktype_name(model)} AS ({type_declaration})"
    )


def drop_pktype(model):
    op.execute(f"DROP TYPE IF EXISTS {_pktype_name(model)}")


def upgrade():
    from swpt_creditors import models

    create_pktype(models.Creditor)
    create_pktype(models.LogEntry)
    create_pktype(models.LedgerEntry)
    create_pktype(models.CommittedTransfer)
    create_pktype(models.AccountData)
    create_pktype(models.ConfigureAccountSignal)
    create_pktype(models.PrepareTransferSignal)
    create_pktype(models.FinalizeTransferSignal)
    create_pktype(models.UpdatedLedgerSignal)
    create_pktype(models.UpdatedPolicySignal)
    create_pktype(models.UpdatedFlagsSignal)
    create_pktype(models.RejectedConfigSignal)


def downgrade():
    from swpt_creditors import models

    drop_pktype(models.Creditor)
    drop_pktype(models.LogEntry)
    drop_pktype(models.LedgerEntry)
    drop_pktype(models.CommittedTransfer)
    drop_pktype(models.AccountData)
    drop_pktype(models.ConfigureAccountSignal)
    drop_pktype(models.PrepareTransferSignal)
    drop_pktype(models.FinalizeTransferSignal)
    drop_pktype(models.UpdatedLedgerSignal)
    drop_pktype(models.UpdatedPolicySignal)
    drop_pktype(models.UpdatedFlagsSignal)
    drop_pktype(models.RejectedConfigSignal)
