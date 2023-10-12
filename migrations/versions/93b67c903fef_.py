"""empty message

Revision ID: 93b67c903fef
Revises: 4260660af5b1
Create Date: 2023-10-12 19:01:18.500442

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '93b67c903fef'
down_revision = '4260660af5b1'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('updated_flags_signal',
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('update_id', sa.BigInteger(), nullable=False),
    sa.Column('config_flags', sa.BigInteger(), nullable=False),
    sa.Column('ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('inserted_at', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('creditor_id', 'debtor_id', 'update_id')
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('updated_flags_signal')
    # ### end Alembic commands ###
