"""empty message

Revision ID: 8d8c816257ce
Revises: 
Create Date: 2020-01-09 16:24:33.153830

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.schema import Sequence, CreateSequence, DropSequence


# revision identifiers, used by Alembic.
revision = '8d8c816257ce'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute(CreateSequence(Sequence('coordinator_request_id_seq')))
    op.execute(CreateSequence(Sequence('creditor_reservation_id_seq')))


def downgrade():
    op.execute(DropSequence(Sequence('coordinator_request_id_seq')))
    op.execute(DropSequence(Sequence('creditor_reservation_id_seq')))
