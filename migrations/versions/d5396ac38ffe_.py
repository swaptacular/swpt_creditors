"""empty message

Revision ID: d5396ac38ffe
Revises: 706698ff9697
Create Date: 2024-07-29 18:38:51.854841

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'd5396ac38ffe'
down_revision = '706698ff9697'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('usage_stats',
    sa.Column('creditor_id', sa.BigInteger(), autoincrement=False, nullable=False),
    sa.Column('accounts_count', sa.SmallInteger(), server_default='0', nullable=False),
    sa.Column('transfers_count', sa.SmallInteger(), server_default='0', nullable=False),
    sa.Column('reconfigs_count', sa.SmallInteger(), server_default='0', nullable=False),
    sa.Column('reconfigs_reset_at', sa.Integer(), server_default='0', nullable=False, comment='Indicates when the `reconfigs_count` filed should be reset to zero. The value is represented as "Unix time" divided by 3600. That is: number of hours after 1970-01-01 00:00:00Z.'),
    sa.Column('initiations_count', sa.SmallInteger(), server_default='0', nullable=False),
    sa.Column('initiations_reset_at', sa.Integer(), server_default='0', nullable=False, comment='Indicates when the `initiations_count` filed should be reset to zero. The value is represented as "Unix time" divided by 3600. That is: number of hours after 1970-01-01 00:00:00Z.'),
    sa.ForeignKeyConstraint(['creditor_id'], ['creditor.creditor_id'], ondelete='CASCADE'),
    sa.PrimaryKeyConstraint('creditor_id'),
    comment='Usage statistics that are used to protect against DOS attacks.'
    )
    # ### end Alembic commands ###

    # Set the fillfactor for the table.
    op.execute('ALTER TABLE usage_stats SET (fillfactor = 75)')


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('usage_stats')
    # ### end Alembic commands ###
