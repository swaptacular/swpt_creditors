import pytest
import math
from marshmallow import ValidationError
from datetime import date, datetime, timezone
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
        peg_debtor_identity='https://example.com/gold',
        peg_debtor_home_url='https://example.com/debtor-home-url',
        peg_debtor_id=-2,
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    ads = schemas.AccountDisplaySchema(context=CONTEXT)
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': '/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'debtorName': 'Test Debtor',
        'ownUnit': 'XXX',
        'ownUnitPreference': 0,
        'peg': {
            'type': 'CurrencyPeg',
            'display': {'uri': '/creditors/1/accounts/18446744073709551614/display'},
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
            'debtorHomeUrl': 'https://example.com/debtor-home-url',
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
    ad.peg_debtor_home_url = None
    ad.peg_debtor_id = None
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': '/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'ownUnitPreference': 0,
        'peg': {
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
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
        'uri': '/creditors/1/accounts/18446744073709551615/display',
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
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
            'exchangeRate': 1.5,
        },
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
    })
    assert data == {
        'type': 'AccountDisplay',
        'own_unit_preference': 1,
        'amount_divisor': 100.0,
        'decimal_places': 2,
        'hide': False,
        'optional_own_unit': 'XXX',
        'optional_debtor_name': 'Test Debtor',
        'optional_peg': {
            'type': 'CurrencyPeg',
            'exchange_rate': 1.5,
            'debtor_identity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
        },
    }

    with pytest.raises(ValidationError):
        ads.load({'type': 'WrongType'})

    with pytest.raises(ValidationError, match='Can not set ownUnit without debtorName.'):
        ads.load({'ownUnit': 'USD'})

    with pytest.raises(ValidationError, match='Length must be between 1 and 4.'):
        ads.load({'debtorName': 'Test Debtor', 'ownUnit': 1000 * 'x'})

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
        ads.load({'debtorName': 1000 * 'x'})

    with pytest.raises(ValidationError, match='Can not set peg without debtorName.'):
        ads.load({
            'peg': {
                'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
                'exchangeRate': 1.5,
            }
        })

    with pytest.raises(ValidationError, match='Invalid type.'):
        ads.load({
            'debtorName': 'Test Debtor',
            'peg': {
                'type': 'WrongType',
                'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
                'exchangeRate': 1.5,
            }
        })

    with pytest.raises(ValidationError):
        ads.load({'peg': {
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
            'exchangeRate': -1.5,
        }})

    with pytest.raises(ValidationError):
        ads.load({'peg': {
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'https://example.com/gold'},
            'exchangeRate': -1.5,
        }})

    with pytest.raises(ValidationError):
        ads.load({'peg': {
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 1000 * 'x'},
            'exchangeRate': 1.5,
        }})


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
        'uri': '/creditors/1/accounts/18446744073709551615/exchange',
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
        'uri': '/creditors/1/accounts/18446744073709551615/exchange',
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
        'minPrincipal': 1000,
        'maxPrincipal': 5000,
        'policy': 'test policy',
    })
    assert data == {
        'type': 'AccountExchange',
        'min_principal': 1000,
        'max_principal': 5000,
        'optional_policy': 'test policy',
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
        account_identity='https://example.com/USD/accounts/123',
        interest_rate=11.0,
        interest_rate_changed_at_ts=datetime(2020, 1, 2),
        debtor_info_sha256=32 * b'\x01',
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    aks = schemas.AccountKnowledgeSchema(context=CONTEXT)
    assert aks.dump(ak) == {
        'type': 'AccountKnowledge',
        'uri': '/creditors/1/accounts/18446744073709551615/knowledge',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'accountIdentity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
        'debtorInfoSha256': 32 * '01',
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ak.account_identity = None
    ak.debtor_info_sha256 = None
    assert aks.dump(ak) == {
        'type': 'AccountKnowledge',
        'uri': '/creditors/1/accounts/18446744073709551615/knowledge',
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
        'interest_rate_changed_at_ts': models.TS0,
    }

    data = aks.load({
        'type': 'AccountKnowledge',
        'accountIdentity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
        'debtorInfoSha256': 16 * 'BA01',
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
    })
    assert data == {
        'type': 'AccountKnowledge',
        'interest_rate': 11.0,
        'interest_rate_changed_at_ts': datetime(2020, 1, 2),
        'optional_account_identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
        'optional_debtor_info_sha256': 16 * 'BA01',
    }

    with pytest.raises(ValidationError):
        aks.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        aks.load({'identity': {'type': 'AccountIdentity', 'uri': 1000 * 'x'}})

    with pytest.raises(ValidationError):
        aks.load({'debtorInfoSha256': 63 * '0'})

    with pytest.raises(ValidationError):
        aks.load({'debtorInfoSha256': 64 * 'g'})

    with pytest.raises(ValidationError):
        aks.load({'debtorInfoSha256': 64 * 'f'})


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
        'uri': '/creditors/1/accounts/18446744073709551615/config',
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
        'uri': '/creditors/1/accounts/18446744073709551615/config',
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
        debtor_info_url=None,
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
        'uri': '/creditors/1/accounts/18446744073709551615/info',
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
    ad.debtor_info_url = 'https://example.com/debtor'
    ad.config_error = 'TEST_ERROR'
    ad.is_scheduled_for_deletion = True
    ad.is_config_effectual = True
    ad.has_server_account = False
    assert ais.dump(ad) == {
        'type': 'AccountInfo',
        'uri': '/creditors/1/accounts/18446744073709551615/info',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'interestRate': 0.0,
        'interestRateChangedAt': '2000-01-01T00:00:00',
        'overflown': False,
        'safeToDelete': True,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
        'accountIdentity': {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/!bm90IFVSTCBzYWZl'},
        'configError': 'TEST_ERROR',
        'debtorInfoUrl': 'https://example.com/debtor',
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
        'uri': '/creditors/1/accounts/18446744073709551615/',
        'accountList': {'uri': '/creditors/1/account-list'},
        'createdAt': account.created_at_ts.isoformat(),
        'latestUpdateId': account.latest_update_id,
        'latestUpdateAt': account.latest_update_ts.isoformat(),
        'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:18446744073709551615'},
        'display': ads.dump(account.display),
        'config': acs.dump(account.config),
        'info': ais.dump(account.data),
        'ledger': als.dump(account.data),
        'exchange': aes.dump(account.exchange),
        'knowledge': aks.dump(account.knowledge),
    }


def test_serialize_currency_peg(app):
    cp = {
        'type': 'CurrencyPeg',
        'debtor_identity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
        'optional_debtor_home_url': 'http://example.com/debtor-home-url',
        'exchange_rate': 2.5,
        'display': {'uri': '/creditors/2/accounts/11/display'}
    }
    cps = schemas.CurrencyPegSchema()
    assert cps.dump(cp) == {
        'type': 'CurrencyPeg',
        'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
        'debtorHomeUrl': 'http://example.com/debtor-home-url',
        'exchangeRate': 2.5,
        'display': {'uri': '/creditors/2/accounts/11/display'}
    }

    del cp['optional_debtor_home_url']
    del cp['display']
    assert cps.dump(cp) == {
        'type': 'CurrencyPeg',
        'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
        'exchangeRate': 2.5,
    }


def test_deserialize_currency_peg(app):
    cps = schemas.CurrencyPegSchema()

    data = cps.load({
        'debtorIdentity': {'uri': 'swpt:111'},
        'exchangeRate': 2.5,
    })
    assert data == {
        'type': 'CurrencyPeg',
        'debtor_identity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
        'exchange_rate': 2.5,
    }

    data = cps.load({
        'type': 'CurrencyPeg',
        'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
        'exchangeRate': 2.5,
        'debtorHomeUrl': 'http://example.com/debtor-home-url',
    })
    assert data == {
        'type': 'CurrencyPeg',
        'debtor_identity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
        'exchange_rate': 2.5,
        'optional_debtor_home_url': 'http://example.com/debtor-home-url',
    }

    with pytest.raises(ValidationError):
        cps.load({
            'type': 'WrongType',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
            'exchangeRate': 2.5,
        })

    with pytest.raises(ValidationError):
        cps.load({
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 1000 * 'x'},
            'exchangeRate': 2.5,
        })

    with pytest.raises(ValidationError):
        cps.load({
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
            'exchangeRate': -0.01,
        })

    with pytest.raises(ValidationError, match='Not a valid URL.'):
        cps.load({
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
            'exchangeRate': 2.5,
            'debtorHomeUrl': '',
        })

    with pytest.raises(ValidationError, match='Longer than maximum length 200.'):
        cps.load({
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:111'},
            'exchangeRate': 2.5,
            'debtorHomeUrl': 'http://example.com/{}'.format(1000 * 'x'),
        })


def test_serialize_account_ledger(app):
    ad = models.AccountData(
        creditor_id=C_ID,
        debtor_id=D_ID,
        creation_date=datetime(2019, 1, 1),
        last_change_ts=datetime(2019, 1, 3, tzinfo=timezone.utc),
        last_change_seqnum=-5,
        principal=1000,
        interest=11.0,
        last_transfer_number=123,
        last_transfer_committed_at_ts=datetime(2019, 1, 2),
        last_config_ts=datetime(2019, 1, 5),
        last_config_seqnum=5,
        last_heartbeat_ts=datetime(2020, 1, 3),
        interest_rate=0.0,
        last_interest_rate_change_ts=datetime(2000, 1, 1),
        status_flags=models.AccountData.STATUS_OVERFLOWN_FLAG,
        account_identity='',
        debtor_info_url=None,
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
    als = schemas.AccountLedgerSchema(context=CONTEXT)
    assert als.dump(ad) == {
        'type': 'AccountLedger',
        'uri': '/creditors/1/accounts/18446744073709551615/ledger',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'principal': 999,
        'interest': 11,
        'entries': {
            'type': 'PaginatedList',
            'itemsType': 'LedgerEntry',
            'first': '/creditors/1/accounts/18446744073709551615/entries?prev=3',
        },
        'latestUpdateId': 2,
        'latestUpdateAt': '2020-01-02T00:00:00',
    }

    ad.interest_rate = 7.0
    assert als.dump(ad)['interest'] > 11

    ad.interest = math.nan
    assert als.dump(ad)['interest'] == 0

    ad.interest = 1e30
    assert als.dump(ad)['interest'] == models.MAX_INT64

    ad.interest = -1e30
    assert als.dump(ad)['interest'] == models.MIN_INT64

    ad.interest = 0.0
    ad.interest_rate = -100.0
    assert als.dump(ad)['interest'] == -1000


def test_serialize_ledger_enty(db_session, app):
    le = models.LedgerEntry(
        creditor_id=C_ID,
        debtor_id=D_ID,
        entry_id=2,
        creation_date=date(1970, 1, 5),
        transfer_number=666,
        aquired_amount=1000,
        principal=3000,
        added_at_ts=datetime(2020, 1, 2),
        previous_entry_id=1,
    )
    les = schemas.LedgerEntrySchema(context=CONTEXT)
    assert les.dump(le) == {
        'type': 'LedgerEntry',
        'ledger': {'uri': '/creditors/1/accounts/18446744073709551615/ledger'},
        'entryId': 2,
        'previousEntryId': 1,
        'principal': 3000,
        'transfer': {'uri': '/creditors/1/accounts/18446744073709551615/transfers/4-666'},
        'aquiredAmount': 1000,
        'addedAt': '2020-01-02T00:00:00',
    }

    le.previous_entry_id = None
    le.creation_date = None
    assert les.dump(le) == {
        'type': 'LedgerEntry',
        'ledger': {'uri': '/creditors/1/accounts/18446744073709551615/ledger'},
        'entryId': 2,
        'principal': 3000,
        'aquiredAmount': 1000,
        'addedAt': '2020-01-02T00:00:00',
    }

    le.previous_entry_id = None
    le.creation_date = date(2000, 1, 1)
    le.transfer_number = None
    assert les.dump(le) == {
        'type': 'LedgerEntry',
        'ledger': {'uri': '/creditors/1/accounts/18446744073709551615/ledger'},
        'entryId': 2,
        'principal': 3000,
        'aquiredAmount': 1000,
        'addedAt': '2020-01-02T00:00:00',
    }
