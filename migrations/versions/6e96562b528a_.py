"""empty message

Revision ID: 6e96562b528a
Revises: ff97ba125d5d
Create Date: 2024-05-14 17:23:27.651608

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '6e96562b528a'
down_revision = 'ff97ba125d5d'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('running_transfer',
                    column_name='final_interest_rate_ts',
                    existing_nullable=True,
                    nullable=False)
    op.alter_column('prepare_transfer_signal',
                    column_name='final_interest_rate_ts',
                    existing_nullable=True,
                    nullable=False)


def downgrade():
    op.alter_column('running_transfer',
                    column_name='final_interest_rate_ts',
                    existing_nullable=False,
                    nullable=True)
    op.alter_column('prepare_transfer_signal',
                    column_name='final_interest_rate_ts',
                    existing_nullable=False,
                    nullable=True)
