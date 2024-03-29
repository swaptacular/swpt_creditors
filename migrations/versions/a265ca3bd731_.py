"""empty message

Revision ID: a265ca3bd731
Revises: ced8680106a7
Create Date: 2021-12-16 18:21:57.838731

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'a265ca3bd731'
down_revision = 'ced8680106a7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('account_display',
                    column_name='hide',
                    new_column_name='known_debtor',
                    existing_type=sa.BOOLEAN(),
                    existing_nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('account_display',
                    column_name='known_debtor',
                    new_column_name='hide',
                    existing_type=sa.BOOLEAN(),
                    existing_nullable=False)
    # ### end Alembic commands ###
