from datetime import datetime
from swpt_creditors import schemas
from swpt_creditors import models
from swpt_creditors.routes import CONTEXT

D_ID = -1
C_ID = 1


def test_account_display_schema(app, current_ts):
    ad = models.AccountDisplay(
        creditor_id=C_ID,
        debtor_id=D_ID,
        debtor_name='Test Debtor',
        amount_divisor=100.0,
        decimal_places=2,
        own_unit='XXX',
        own_unit_preference=0,
        hide=False,
        peg_exchange_rate=1.0,
        peg_debtor_uri='https://example.com/gold',
        peg_debtor_id=-2,
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    ads = schemas.AccountDisplaySchema(context=CONTEXT)
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'debtorName': 'Test Debtor',
        'ownUnit': 'XXX',
        'ownUnitPreference': 0,
        'peg': {
            'type': 'AccountPeg',
            'display': {'uri': '/creditors/1/accounts/18446744073709551614/display'},
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 1.0,
        },
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ad.debtor_name = None
    ad.own_unit = None
    ad.peg_debtor_id = None
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'ownUnitPreference': 0,
        'peg': {
            'type': 'AccountPeg',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 1.0,
        },
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ad.peg_exchange_rate = None
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'ownUnitPreference': 0,
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }
