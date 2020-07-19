import pytest
from marshmallow import ValidationError
from datetime import datetime
from swpt_creditors import schemas
from swpt_creditors import models
from swpt_creditors import procedures
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
        ads.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        ads.load({'ownUnit': 1000 * 'x'})

    with pytest.raises(ValidationError):
        ads.load({'ownUnitPreference': models.MIN_INT32 - 1})

    with pytest.raises(ValidationError):
        ads.load({'ownUnitPreference': models.MAX_INT32 + 1})

    with pytest.raises(ValidationError):
        ads.load({'amountDivisor': 0.0})

    with pytest.raises(ValidationError):
        ads.load({'amountDivisor': -0.01})

    with pytest.raises(ValidationError):
        ads.load({'decimalPlaces': 10000})

    with pytest.raises(ValidationError):
        ads.load({'debtor_name': 1000 * 'x'})

    with pytest.raises(ValidationError):
        ads.load({'peg': {
            'type': 'WrongType',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 1.5,
        }})

    with pytest.raises(ValidationError):
        ads.load({'peg': {'debtor': {'uri': 'https://example.com/gold'}, 'exchangeRate': -1.5}})

    with pytest.raises(ValidationError):
        ads.load({'peg': {'debtor': {'uri': 'https://example.com/gold'}, 'exchangeRate': -1.5}})

    with pytest.raises(ValidationError):
        ads.load({'peg': {'debtor': {'uri': 1000 * 'x'}, 'exchangeRate': 1.5}})


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
        aes.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        aes.load({'minPrincipal': 5000, 'maxPrincipal': 1000})

    with pytest.raises(ValidationError):
        aes.load({'minPrincipal': models.MIN_INT64 - 1})

    with pytest.raises(ValidationError):
        aes.load({'maxPrincipal': models.MAX_INT64 + 1})

    with pytest.raises(ValidationError):
        aes.load({'policy': 1000 * 'x'})


def test_serialize_account_knowledge(app):
    ak = models.AccountKnowledge(
        creditor_id=C_ID,
        debtor_id=D_ID,
        identity_uri='https://example.com/USD/accounts/123',
        interest_rate=11.0,
        interest_rate_changed_at_ts=datetime(2020, 1, 2),
        debtor_url='https://example.com/USD',
        peg_exchange_rate=2000.0,
        peg_debtor_uri='https://example.com/gold',
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    aks = schemas.AccountKnowledgeSchema(context=CONTEXT)
    assert aks.dump(ak) == {
        'type': 'AccountKnowledge',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/knowledge',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'identity': {'uri': 'https://example.com/USD/accounts/123'},
        'currencyPeg': {
            'type': 'CurrencyPeg',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 2000.0,
        },
        'debtorUrl': 'https://example.com/USD',
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ak.identity_uri = None
    ak.debtor_url = None
    ak.peg_exchange_rate = None
    assert aks.dump(ak) == {
        'type': 'AccountKnowledge',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/knowledge',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_deserialize_account_knowledge(app):
    aks = schemas.AccountKnowledgeSchema(context=CONTEXT)

    data = aks.load({})
    assert data == {
        'type': 'AccountKnowledge',
        'interest_rate': 0.0,
        'interest_rate_changed_at_ts': models.BEGINNING_OF_TIME,
    }

    data = aks.load({
        'type': 'AccountKnowledge',
        'identity': {'uri': 'https://example.com/USD/accounts/123'},
        'currencyPeg': {
            'type': 'CurrencyPeg',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchangeRate': 2000.0,
        },
        'debtorUrl': 'https://example.com/USD',
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
    })
    assert data == {
        'type': 'AccountKnowledge',
        'identity': {'uri': 'https://example.com/USD/accounts/123'},
        'currency_peg': {
            'type': 'CurrencyPeg',
            'debtor': {'uri': 'https://example.com/gold'},
            'exchange_rate': 2000.0,
        },
        'debtor_url': 'https://example.com/USD',
        'interest_rate': 11.0,
        'interest_rate_changed_at_ts': datetime(2020, 1, 2),
    }

    with pytest.raises(ValidationError):
        aks.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        aks.load({'identity': {'uri': 1000 * 'x'}})

    with pytest.raises(ValidationError):
        aks.load({'debtorUrl': 1000 * 'x'})


def test_serialize_account_config(app):
    ac = models.AccountConfig(
        creditor_id=C_ID,
        debtor_id=D_ID,
        negligible_amount=101.0,
        config='test config',
        config_flags=models.AccountConfig.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        allow_unsafe_deletion=True,
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    acs = schemas.AccountConfigSchema(context=CONTEXT)
    assert acs.dump(ac) == {
        'type': 'AccountConfig',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/config',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'negligibleAmount': 101.0,
        'scheduledForDeletion': True,
        'allowUnsafeDeletion': True,
        'config': 'test config',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ac.negligible_amount = 1e30
    ac.config = ''
    ac.config_flags = 0
    ac.allow_unsafe_deletion = False
    assert acs.dump(ac) == {
        'type': 'AccountConfig',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/config',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'negligibleAmount': 1e30,
        'scheduledForDeletion': False,
        'allowUnsafeDeletion': False,
        'config': '',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_deserialize_account_config(app):
    acs = schemas.AccountConfigSchema(context=CONTEXT)

    data = acs.load({
        'negligibleAmount': 1.0,
        'scheduledForDeletion': True,
    })
    assert data == {
        'type': 'AccountConfig',
        'negligible_amount': 1.0,
        'is_scheduled_for_deletion': True,
        'allow_unsafe_deletion': False,
        'config': '',
    }

    data = acs.load({
        'type': 'AccountConfig',
        'negligibleAmount': 1.0,
        'allowUnsafeDeletion': True,
        'scheduledForDeletion': False,
        'config': 'test config',
    })
    assert data == {
        'type': 'AccountConfig',
        'negligible_amount': 1.0,
        'is_scheduled_for_deletion': False,
        'allow_unsafe_deletion': True,
        'config': 'test config',
    }

    with pytest.raises(ValidationError):
        acs.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        acs.load({'config': {'uri': 1000 * 'x'}})


def test_serialize_account_info(app):
    ad = models.AccountData(
        creditor_id=C_ID,
        debtor_id=D_ID,
        creation_date=datetime(2019, 1, 1),
        last_change_ts=datetime(2019, 1, 3),
        last_change_seqnum=-5,
        principal=1000,
        interest=11.0,
        last_transfer_number=123,
        last_transfer_committed_at_ts=datetime(2019, 1, 2),
        last_config_ts=datetime(2019, 1, 5),
        last_config_seqnum=5,
        last_heartbeat_ts=datetime(2020, 1, 3),
        interest_rate=7.0,
        last_interest_rate_change_ts=datetime(2000, 1, 1),
        status_flags=models.AccountData.STATUS_OVERFLOWN_FLAG,
        account_identity='',
        debtor_url=None,
        config_error=None,
        is_config_effectual=True,
        is_scheduled_for_deletion=False,
        has_server_account=True,
        info_latest_update_id=1,
        info_latest_update_ts=datetime(2020, 1, 1),
        ledger_principal=999,
        ledger_last_transfer_number=122,
        ledger_latest_update_id=2,
        ledger_latest_update_ts=datetime(2020, 1, 2),
    )
    ais = schemas.AccountInfoSchema(context=CONTEXT)
    assert ais.dump(ad) == {
        'type': 'AccountInfo',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/info',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'interestRate': 7.0,
        'interestRateChangedAt': '2000-01-01T00:00:00',
        'overflown': True,
        'safeToDelete': False,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',

    }

    ad.interest_rate = 0.0
    ad.status_flags = 0
    ad.account_identity = 'not URL safe'
    ad.debtor_url = 'https://example.com/debtor'
    ad.config_error = 'TEST_ERROR'
    ad.is_scheduled_for_deletion = True
    ad.is_config_effectual = True
    ad.has_server_account = False
    assert ais.dump(ad) == {
        'type': 'AccountInfo',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/info',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'interestRate': 0.0,
        'interestRateChangedAt': '2000-01-01T00:00:00',
        'overflown': False,
        'safeToDelete': True,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
        'identity': {'uri': 'swpt:18446744073709551615/!bm90IFVSTCBzYWZl'},
        'configError': 'TEST_ERROR',
        'debtorUrl': 'https://example.com/debtor',
    }


def test_serialize_account(db_session):
    assert procedures.create_new_creditor(C_ID)
    assert procedures.create_account(C_ID, D_ID)
    account = models.Account.get_instance((C_ID, D_ID))
    account_schema = schemas.AccountSchema(context=CONTEXT)
    ads = schemas.AccountDisplaySchema(context=CONTEXT)
    acs = schemas.AccountConfigSchema(context=CONTEXT)
    ais = schemas.AccountInfoSchema(context=CONTEXT)
    als = schemas.AccountLedgerSchema(context=CONTEXT)
    aes = schemas.AccountExchangeSchema(context=CONTEXT)
    aks = schemas.AccountKnowledgeSchema(context=CONTEXT)
    assert account_schema.dump(account) == {
        'type': 'Account',
        'uri': 'http://example.com/creditors/1/accounts/18446744073709551615/',
        'accountList': {'uri': '/creditors/1/account-list'},
        'createdAt': account.created_at_ts.isoformat(),
        'latestUpdateId': account.latest_update_id,
        'latestUpdateAt': account.latest_update_ts.isoformat(),
        'debtor': {'uri': 'swpt:18446744073709551615'},
        'display': ads.dump(account.display),
        'config': acs.dump(account.config),
        'info': ais.dump(account.data),
        'ledger': als.dump(account.data),
        'exchange': aes.dump(account.exchange),
        'knowledge': aks.dump(account.knowledge),
    }
