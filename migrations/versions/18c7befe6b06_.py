"""empty message

Revision ID: 18c7befe6b06
Revises: 7a0a2047ec14
Create Date: 2020-05-20 22:31:09.307048

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '18c7befe6b06'
down_revision = '7a0a2047ec14'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('account_config', sa.Column('account_identity', sa.String(), nullable=True, comment='The value of the `account_identity` field from the first received `AccountChangeSignal` for the account.'))
    op.drop_column('account_config', 'creditor_identity')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('account_config', sa.Column('creditor_identity', sa.VARCHAR(), autoincrement=False, nullable=True, comment='The value of the `creditor_identity` field from the first received `AccountChangeSignal` for the account. Once set, must never change.'))
    op.drop_column('account_config', 'account_identity')
    # ### end Alembic commands ###
