from datetime import date
from uuid import UUID
from swpt_creditors import procedures as p

D_ID = -1
C_ID = 1
TEST_UUID = UUID('123e4567-e89b-12d3-a456-426655440000')
TEST_UUID2 = UUID('123e4567-e89b-12d3-a456-426655440001')
RECIPIENT_URI = 'https://example.com/creditors/1'


def test_process_pending_committed_transfers(db_session, current_ts):
    ny2020 = date(2020, 1, 1)
    p.process_committed_transfer_signal(D_ID, C_ID, 1, ny2020, 'direct', 666, current_ts, 1000, {}, 1000)
    assert p.process_pending_committed_transfers(C_ID, D_ID)