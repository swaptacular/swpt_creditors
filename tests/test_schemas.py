import pytest
from marshmallow import ValidationError
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

    with pytest.raises(ValidationError):
        data = ads.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        data = ads.load({'ownUnit': 1000 * 'x'})

    with pytest.raises(ValidationError):
        data = ads.load({'ownUnitPreference': models.MIN_INT32 - 1})

    with pytest.raises(ValidationError):
        data = ads.load({'ownUnitPreference': models.MAX_INT32 + 1})

    with pytest.raises(ValidationError):
        data = ads.load({'amountDivisor': 0.0})

    with pytest.raises(ValidationError):
        data = ads.load({'amountDivisor': -0.01})

    with pytest.raises(ValidationError):
        data = ads.load({'decimalPlaces': 10000})

    with pytest.raises(ValidationError):
        data = ads.load({'debtor_name': 1000 * 'x'})

    with pytest.raises(ValidationError):
        data = ads.load({'peg': {
            'type': 'WrongType',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 1.5,
        }})

    with pytest.raises(ValidationError):
        data = ads.load({'peg': {'debtor': {'uri': 'https://example.com/gold'}, 'exchangeRate': -1.5}})

    with pytest.raises(ValidationError):
        data = ads.load({'peg': {'debtor': {'uri': 'https://example.com/gold'}, 'exchangeRate': -1.5}})

    with pytest.raises(ValidationError):
        data = ads.load({'peg': {'debtor': {'uri': 1000 * 'x'}, 'exchangeRate': 1.5}})


def test_serialize_account_exchange(app):
    ae = models.AccountExchange(
        creditor_id=C_ID,
        debtor_id=D_ID,
        policy='test policy',
        min_principal=1000,
        max_principal=5000,
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    aes = schemas.AccountExchangeSchema(context=CONTEXT)
    assert aes.dump(ae) == {
        'type': 'AccountExchange',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/exchange',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'policy': 'test policy',
        'minPrincipal': 1000,
        'maxPrincipal': 5000,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ae.policy = None
    assert aes.dump(ae) == {
        'type': 'AccountExchange',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/exchange',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'minPrincipal': 1000,
        'maxPrincipal': 5000,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_deserialize_account_exchange(app):
    aes = schemas.AccountExchangeSchema(context=CONTEXT)

    data = aes.load({})
    assert data == {
        'type': 'AccountExchange',
        'min_principal': models.MIN_INT64,
        'max_principal': models.MAX_INT64,
    }

    data = aes.load({
        'type': 'AccountExchange',
        'policy': 'test policy',
        'minPrincipal': 1000,
        'maxPrincipal': 5000,
    })
    assert data == {
        'type': 'AccountExchange',
        'policy': 'test policy',
        'min_principal': 1000,
        'max_principal': 5000,
    }

    with pytest.raises(ValidationError):
        data = aes.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        data = aes.load({'minPrincipal': 5000, 'maxPrincipal': 1000})

    with pytest.raises(ValidationError):
        data = aes.load({'minPrincipal': models.MIN_INT64 - 1})

    with pytest.raises(ValidationError):
        data = aes.load({'maxPrincipal': models.MAX_INT64 + 1})

    with pytest.raises(ValidationError):
        data = aes.load({'policy': 1000 * 'x'})
