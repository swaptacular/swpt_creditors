"""empty message

Revision ID: 5314a555b325
Revises: 8c56a59f4f05
Create Date: 2021-10-13 00:46:51.949353

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence


# revision identifiers, used by Alembic.
revision = '5314a555b325'
down_revision = '8c56a59f4f05'
branch_labels = None
depends_on = None


def upgrade():
    op.execute(CreateSequence(Sequence('object_update_id_seq')))
    op.alter_column('account', 'latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))
    op.alter_column('account_data', 'config_latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))
    op.alter_column('account_data', 'info_latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))
    op.alter_column('account_data', 'ledger_latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))
    op.alter_column('account_knowledge', 'latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))
    op.alter_column('account_exchange', 'latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))
    op.alter_column('account_display', 'latest_update_id', server_default=sa.text("nextval('object_update_id_seq')"))


def downgrade():
    op.alter_column('account', 'latest_update_id', server_default=None)
    op.alter_column('account_data', 'config_latest_update_id', server_default=None)
    op.alter_column('account_data', 'info_latest_update_id', server_default=None)
    op.alter_column('account_data', 'ledger_latest_update_id', server_default=None)
    op.alter_column('account_knowledge', 'latest_update_id', server_default=None)
    op.alter_column('account_exchange', 'latest_update_id', server_default=None)
    op.alter_column('account_display', 'latest_update_id', server_default=None)
    op.execute(DropSequence(Sequence('object_update_id_seq')))
