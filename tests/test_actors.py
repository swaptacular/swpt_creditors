from swpt_creditors import actors as a

D_ID = -1
C_ID = 1


def test_on_account_transfer_signal(db_session):
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_seqnum=(1 << 40) + 1,
        coordinator_type='direct',
        other_creditor_id=666,
        committed_at_ts='2019-10-01T00:00:00Z',
        committed_amount=1000,
        transfer_message='{"message": "test"}',
        transfer_flags=0,
        account_creation_date='2020-01-02',
        account_new_principal=1000,
        previous_transfer_seqnum=(1 << 40),
        system_flags=0,
        real_creditor_id=C_ID,
    )
    a.on_account_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_seqnum=(1 << 40) + 1,
        coordinator_type='direct',
        other_creditor_id=666,
        committed_at_ts='2019-10-01T00:00:00Z',
        committed_amount=1000,
        transfer_message='test',
        transfer_flags=0,
        account_creation_date='2020-01-02',
        account_new_principal=1000,
        previous_transfer_seqnum=(1 << 40),
        system_flags=0,
        real_creditor_id=C_ID,
    )
