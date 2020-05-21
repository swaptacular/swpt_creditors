from swpt_creditors import actors as a

D_ID = -1
C_ID = 1


def test_on_account_transfer_signal(db_session):
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_number=1,
        coordinator_type='direct',
        committed_at='2019-10-01T00:00:00Z',
        amount=1000,
        transfer_message='{"message": "test"}',
        transfer_flags=0,
        creation_date='2020-01-02',
        principal=1000,
        previous_transfer_number=0,
        sender='666',
        recipient=str(C_ID),
    )
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_number=1,
        coordinator_type='direct',
        committed_at='2019-10-01T00:00:00Z',
        amount=1000,
        transfer_message='test',
        transfer_flags=0,
        creation_date='2020-01-02',
        principal=1000,
        previous_transfer_number=0,
        sender='666',
        recipient=str(C_ID),
    )
