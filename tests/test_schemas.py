from datetime import datetime
from swpt_creditors import schemas
from swpt_creditors import models
from swpt_creditors.routes import CONTEXT

D_ID = -1
C_ID = 1


def test_serialize_account_display(app):
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


def test_deserialize_account_display(app):
    ads = schemas.AccountDisplaySchema(context=CONTEXT)

    data = ads.load({})
    assert data == {
        'type': 'AccountDisplay',
        'own_unit_preference': 0,
        'amount_divisor': 1.0,
        'decimal_places': 0,
        'hide': False,
    }

    data = ads.load({
        'type': 'AccountDisplay',
        'debtorName': 'Test Debtor',
        'ownUnit': 'XXX',
        'ownUnitPreference': 1,
        'peg': {
            'type': 'AccountPeg',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 1.5,
        },
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
    })
    assert data == {
        'type': 'AccountDisplay',
        'own_unit': 'XXX',
        'own_unit_preference': 1,
        'amount_divisor': 100.0,
        'decimal_places': 2,
        'hide': False,
        'debtor_name': 'Test Debtor',
        'peg': {
            'type': 'AccountPeg',
            'exchange_rate': 1.5,
            'debtor': {'uri': 'https://example.com/gold'},
        },
    }
