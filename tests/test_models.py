from datetime import datetime, timezone, timedelta
from swpt_creditors import models as m


def test_account_data(db_session):
    current_ts = datetime.now(tz=timezone.utc)
    ad = m.AccountData(
        principal=0,
        interest=0.0,
        interest_rate=0.0,
        last_change_ts=current_ts - timedelta(days=366),
        config_flags=m.DEFAULT_CONFIG_FLAGS,
    )

    assert ad.is_scheduled_for_deletion is False
    ad.is_scheduled_for_deletion = True
    assert ad.is_scheduled_for_deletion is True
    ad.is_scheduled_for_deletion = False
    assert ad.is_scheduled_for_deletion is False

    assert ad.ledger_interest == 0
    ad.interest = 10.0
    assert ad.ledger_interest == 10
    ad.principal = 990
    assert ad.ledger_interest == 10
    ad.interest_rate = 12.5
    assert abs(ad.ledger_interest - (1000 * 1.125 - 1000 + 10)) < 2
