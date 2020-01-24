from swpt_creditors import actors as a

D_ID = -1
C_ID = 1


def test_on_committed_transfer_signal(db_session):
    a.on_committed_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_seqnum=(1 << 40) + 1,
        coordinator_type='direct',
        other_creditor_id=666,
        committed_at_ts='2019-10-01T00:00:00Z',
        committed_amount=1000,
        transfer_info={'message': 'test'},
        account_creation_date='2020-01-02',
        account_new_principal=1000,
    )
    a.on_committed_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_seqnum=(1 << 40) + 1,
        coordinator_type='direct',
        other_creditor_id=666,
        committed_at_ts='2019-10-01T00:00:00Z',
        committed_amount=1000,
        transfer_info={'message': 'test'},
        account_creation_date='2020-01-02',
        account_new_principal=1000,
    )
