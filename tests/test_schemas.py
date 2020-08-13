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


def test_serialize_creditor(app):
    c = models.Creditor(
        creditor_id=C_ID,
        created_at_date=date(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        latest_log_entry_id=1,
        creditor_latest_update_id=1,
        creditor_latest_update_ts=datetime(2020, 1, 1),
    )
    cs = schemas.CreditorSchema(context=CONTEXT)
    assert cs.dump(c) == {
        'type': 'Creditor',
        'uri': '/creditors/1/',
        'createdOn': '2019-11-30',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_deserialize_creditor(app):
    cs = schemas.CreditorSchema(context=CONTEXT)

    data = cs.load({})
    assert data == {
        'type': 'Creditor',
    }
    data = cs.load({'type': 'Creditor'})
    assert data == {
        'type': 'Creditor',
    }

    with pytest.raises(ValidationError):
        cs.load({'type': 'WrongType'})


def test_serialize_wallet(app):
    c = models.Creditor(
        creditor_id=C_ID,
        created_at_date=date(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        latest_log_entry_id=12345,
    )
    ws = schemas.WalletSchema(context=CONTEXT)
    assert ws.dump(c) == {
        'type': 'Wallet',
        'uri': '/creditors/1/wallet',
        'creditor': {'uri': '/creditors/1/'},
        'accountList': {'uri': '/creditors/1/account-list'},
        'transferList': {'uri': '/creditors/1/transfer-list'},
        'accountLookup': {'uri': '/creditors/1/account-lookup'},
        'debtorLookup': {'uri': '/creditors/1/debtor-lookup'},
        'createAccount': {'uri': '/creditors/1/accounts/'},
        'createTransfer': {'uri': '/creditors/1/transfers/'},
        'log': {
            'type': 'PaginatedList',
            'itemsType': 'LogEntry',
            'first': '/creditors/1/log',
            'forthcoming': '/creditors/1/log?prev=12345',
        },
    }


def test_serialize_log_entry(app):
    le = models.LogEntry(
        creditor_id=C_ID,
        entry_id=12345,
        added_at_ts=datetime(2020, 1, 2),
        previous_entry_id=12344,
        object_type='Account',
        object_uri='/creditors/1/accounts/123/',
        object_update_id=777,
        is_deleted=True,
        data=None,
    )
    les = schemas.LogEntrySchema(context=CONTEXT)
    assert les.dump(le) == {
        'type': 'LogEntry',
        'entryId': 12345,
        'previousEntryId': 12344,
        'addedAt': '2020-01-02T00:00:00',
        'objectType': 'Account',
        'object': {'uri': '/creditors/1/accounts/123/'},
        'objectUpdateId': 777,
        'deleted': True,
    }

    le.previous_entry_id = None
    le.is_deleted = False
    le.data = {'test': 'test', 'list': [1, 2, 3]}
    le.object_update_id = None
    assert les.dump(le) == {
        'type': 'LogEntry',
        'entryId': 12345,
        'addedAt': '2020-01-02T00:00:00',
        'objectType': 'Account',
        'object': {'uri': '/creditors/1/accounts/123/'},
        'data': le.data,
    }


def test_serialize_log_entries_page(app):
    le = models.LogEntry(
        creditor_id=C_ID,
        entry_id=12345,
        added_at_ts=datetime(2020, 1, 2),
        previous_entry_id=12344,
        object_type='Account',
        object_uri='/creditors/1/accounts/123/',
        is_deleted=True,
        data=None,
    )
    lep = {
        'uri': '/test',
        'items': [le],
        'next': '?prev=1',
    }
    les = schemas.LogEntrySchema(context=CONTEXT)
    leps = schemas.LogEntriesPageSchema(context=CONTEXT)
    assert leps.dump(lep) == {
        'type': 'LogEntriesPage',
        'uri': '/test',
        'items': [les.dump(le)],
        'next': '?prev=1',
    }

    del lep['next']
    lep['items'] = []
    lep['forthcoming'] = '?prev=2'
    assert leps.dump(lep) == {
        'type': 'LogEntriesPage',
        'uri': '/test',
        'items': [],
        'forthcoming': '?prev=2'
    }


def test_serialize_account_list(app):
    c = models.Creditor(
        creditor_id=C_ID,
        created_at_date=date(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        latest_log_entry_id=1,
        account_list_latest_update_id=1,
        account_list_latest_update_ts=datetime(2020, 1, 1),
    )
    als = schemas.AccountListSchema(context=CONTEXT)
    assert als.dump(c) == {
        'type': 'AccountList',
        'uri': '/creditors/1/account-list',
        'wallet': {'uri': '/creditors/1/wallet'},
        'itemsType': 'ObjectReference',
        'first': '/creditors/1/accounts/',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_serialize_transfer_list(app):
    c = models.Creditor(
        creditor_id=C_ID,
        created_at_date=date(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        latest_log_entry_id=1,
        transfer_list_latest_update_id=1,
        transfer_list_latest_update_ts=datetime(2020, 1, 1),
    )
    tls = schemas.TransferListSchema(context=CONTEXT)
    assert tls.dump(c) == {
        'type': 'TransferList',
        'uri': '/creditors/1/transfer-list',
        'wallet': {'uri': '/creditors/1/wallet'},
        'itemsType': 'ObjectReference',
        'first': '/creditors/1/transfers/',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


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
        peg_currency_debtor_id=-2,
        peg_account_debtor_id=-2,
        peg_debtor_home_url='https://example.com/debtor-home-url',
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
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:18446744073709551614'},
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
    ad.peg_account_debtor_id = None
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': '/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'ownUnitPreference': 0,
        'peg': {
            'type': 'CurrencyPeg',
            'debtorIdentity': {'type': 'DebtorIdentity', 'uri': 'swpt:18446744073709551614'},
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
    ac = models.AccountData(
        creditor_id=C_ID,
        debtor_id=D_ID,
        negligible_amount=101.0,
        config_flags=models.DEFAULT_CONFIG_FLAGS | models.AccountData.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        allow_unsafe_deletion=True,
        config_latest_update_id=1,
        config_latest_update_ts=datetime(2020, 1, 1),
    )
    acs = schemas.AccountConfigSchema(context=CONTEXT)
    assert acs.dump(ac) == {
        'type': 'AccountConfig',
        'uri': '/creditors/1/accounts/18446744073709551615/config',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'negligibleAmount': 101.0,
        'scheduledForDeletion': True,
        'allowUnsafeDeletion': True,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ac.negligible_amount = models.DEFAULT_NEGLIGIBLE_AMOUNT
    ac.config_flags = 0
    ac.allow_unsafe_deletion = False
    assert acs.dump(ac) == {
        'type': 'AccountConfig',
        'uri': '/creditors/1/accounts/18446744073709551615/config',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'negligibleAmount': models.DEFAULT_NEGLIGIBLE_AMOUNT,
        'scheduledForDeletion': False,
        'allowUnsafeDeletion': False,
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
    }

    data = acs.load({
        'type': 'AccountConfig',
        'negligibleAmount': 1.0,
        'allowUnsafeDeletion': True,
        'scheduledForDeletion': False,
    })
    assert data == {
        'type': 'AccountConfig',
        'negligible_amount': 1.0,
        'is_scheduled_for_deletion': False,
        'allow_unsafe_deletion': True,
    }

    with pytest.raises(ValidationError):
        acs.load({'type': 'WrongType'})


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
        account_id='',
        debtor_info_url=None,
        config_error=None,
        is_config_effectual=True,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
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
    ad.account_id = 'not URL safe'
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
    assert procedures.create_new_account(C_ID, D_ID)
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
        'config': acs.dump(account.data),
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

    del cp['type']
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
        account_id='',
        debtor_info_url=None,
        config_error=None,
        is_config_effectual=True,
        config_flags=models.DEFAULT_CONFIG_FLAGS,
        has_server_account=True,
        info_latest_update_id=1,
        info_latest_update_ts=datetime(2020, 1, 1),
        ledger_principal=999,
        ledger_last_transfer_number=122,
        ledger_latest_update_id=2,
        ledger_latest_update_ts=datetime(2020, 1, 2),
        ledger_latest_entry_id=0,
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
            'first': '/creditors/1/accounts/18446744073709551615/entries?prev=1',
        },
        'latestUpdateId': 2,
        'latestUpdateAt': '2020-01-02T00:00:00',
    }

    ad.ledger_latest_entry_id = 54321
    assert als.dump(ad)['latestEntryId'] == 54321
    assert als.dump(ad)['entries']['first'] == '/creditors/1/accounts/18446744073709551615/entries?prev=54322'

    ad.interest_rate = 7.0
    assert als.dump(ad)['interest'] > 11

    ad.interest = math.nan
    assert als.dump(ad)['interest'] == 0

    ad.interest = 1e20
    assert als.dump(ad)['interest'] == models.MAX_INT64

    ad.interest = -1e20
    assert als.dump(ad)['interest'] == models.MIN_INT64

    ad.interest = 0.0
    ad.interest_rate = -100.0
    assert als.dump(ad)['interest'] == -1000


def test_serialize_ledger_entry(app):
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


def test_serialize_paginated_list(app):
    pl = {
        'items_type': 'String',
        'first': '/first',
        'forthcoming': '/more',
    }
    pls = schemas.PaginatedListSchema(context=CONTEXT)
    assert pls.dump(pl) == {
        'type': 'PaginatedList',
        'itemsType': 'String',
        'first': '/first',
        'forthcoming': '/more',
    }

    del pl['forthcoming']
    assert pls.dump(pl) == {
        'type': 'PaginatedList',
        'itemsType': 'String',
        'first': '/first',
    }


def test_serialize_ledger_entries_page(app):
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
    lep = {
        'uri': '/test',
        'items': [le],
        'next': '?prev=1',
    }
    les = schemas.LedgerEntrySchema(context=CONTEXT)
    leps = schemas.LedgerEntriesPageSchema(context=CONTEXT)
    assert leps.dump(lep) == {
        'type': 'LedgerEntriesPage',
        'uri': '/test',
        'next': '?prev=1',
        'items': [les.dump(le)],
    }

    del lep['next']
    lep['items'] = []
    assert leps.dump(lep) == {
        'type': 'LedgerEntriesPage',
        'uri': '/test',
        'items': [],
    }


def test_serialize_object_references_page(app):
    orp = {
        'uri': '/test',
        'items': [{'uri': '/object1'}, {'uri': '/object2'}],
        'next': '?prev=1',
    }
    orps = schemas.ObjectReferencesPageSchema(context=CONTEXT)
    assert orps.dump(orp) == {
        'type': 'ObjectReferencesPage',
        'uri': '/test',
        'next': '?prev=1',
        'items': [{'uri': '/object1'}, {'uri': '/object2'}],
    }

    del orp['next']
    orp['items'] = []
    assert orps.dump(orp) == {
        'type': 'ObjectReferencesPage',
        'uri': '/test',
        'items': [],
    }


def test_serialize_debtor_identity(app):
    di = {'uri': 'swpt:1'}
    dis = schemas.DebtorIdentitySchema(context=CONTEXT)
    assert dis.dump(di) == {
        'type': 'DebtorIdentity',
        'uri': 'swpt:1',
    }


def test_deserialize_debtor_identity(app):
    dis = schemas.DebtorIdentitySchema(context=CONTEXT)

    data = dis.load({'uri': 'swpt:1'})
    assert data == {
        'type': 'DebtorIdentity',
        'uri': 'swpt:1',
    }

    with pytest.raises(ValidationError):
        dis.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        dis.load({})

    with pytest.raises(ValidationError):
        dis.load({'uri': 1000 * 'x'})


def test_serialize_account_identity(app):
    ai = {'uri': 'swpt:1/2'}
    ais = schemas.AccountIdentitySchema(context=CONTEXT)
    assert ais.dump(ai) == {
        'type': 'AccountIdentity',
        'uri': 'swpt:1/2',
    }


def test_deserialize_account_identity(app):
    ais = schemas.AccountIdentitySchema(context=CONTEXT)

    data = ais.load({'uri': 'swpt:1/2'})
    assert data == {
        'type': 'AccountIdentity',
        'uri': 'swpt:1/2',
    }

    with pytest.raises(ValidationError):
        ais.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        ais.load({})

    with pytest.raises(ValidationError):
        ais.load({'uri': 1000 * 'x'})


def test_deserialize_pagination_parameters(app):
    pps = schemas.PaginationParametersSchema(context=CONTEXT)

    data = pps.load({})
    assert data == {}

    data = pps.load({'prev': 'p', 'stop': 's'})
    assert data == {'prev': 'p', 'stop': 's'}


def test_deserialize_creditor_creation_request(app):
    ccr = schemas.CreditorCreationRequestSchema(context=CONTEXT)
    assert ccr.load({}) == {'type': 'CreditorCreationRequest'}
    assert ccr.load({'type': 'CreditorCreationRequest'}) == {'type': 'CreditorCreationRequest'}

    with pytest.raises(ValidationError):
        ccr.load({'type': 'WrongType'})


def test_serialize_committed_transfer(app):
    ct = models.CommittedTransfer(
        creditor_id=C_ID,
        debtor_id=D_ID,
        creation_date=date(1970, 1, 5),
        transfer_number=666,
        coordinator_type='direct',
        committed_at_ts=datetime(2020, 1, 1),
        acquired_amount=1000,
        transfer_note='{"test": "test", "list": [1, 2, 3]}',
        principal=1500,
        sender_identity='1',
        recipient_identity='1111',
    )
    cts = schemas.CommittedTransferSchema(context=CONTEXT)
    assert cts.dump(ct) == {
        'type': 'CommittedTransfer',
        'uri': '/creditors/1/accounts/18446744073709551615/transfers/4-666',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'coordinator': 'direct',
        'committedAt': '2020-01-01T00:00:00',
        'sender': {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/1'},
        'recipient': {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/1111'},
        'acquiredAmount': 1000,
        'note': {"test": "test", "list": [1, 2, 3]},
    }

    ct.transfer_note = ''
    assert 'note' not in cts.dump(ct)

    ct.transfer_note = 'test'
    assert cts.dump(ct)['note'] == {'type': 'TextMessage', 'content': 'test'}

    ct.transfer_note = '[]'
    assert cts.dump(ct)['note'] == {'type': 'TextMessage', 'content': '[]'}

    # invalid identity
    ct.sender_identity = 1000 * '1'
    ct.recipient_identity = 1000 * '1'
    data = cts.dump(ct)
    assert data['sender'] == {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/!'}
    assert data['recipient'] == {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/!'}


def test_deserialize_log_pagination_params(app):
    ais = schemas.LogPaginationParamsSchema()
    assert ais.load({}) == {'prev': 0}
    assert ais.load({'prev': 0}) == {'prev': 0}
    assert ais.load({'prev': 22}) == {'prev': 22}
    assert ais.load({'prev': models.MAX_INT64}) == {'prev': models.MAX_INT64}

    with pytest.raises(ValidationError):
        ais.load({'prev': -1})

    with pytest.raises(ValidationError):
        ais.load({'prev': models.MAX_INT64 + 1})


def test_deserialize_accounts_pagination_params(app):
    ais = schemas.AccountsPaginationParamsSchema()
    assert ais.load({}) == {}
    assert ais.load({'prev': 0}) == {'prev': 0}
    assert ais.load({'prev': -1}) == {'prev': -1}
    assert ais.load({'prev': 1}) == {'prev': 1}
    assert ais.load({'prev': models.MIN_INT64}) == {'prev': models.MIN_INT64}
    assert ais.load({'prev': models.MAX_INT64}) == {'prev': models.MAX_INT64}

    with pytest.raises(ValidationError):
        ais.load({'prev': models.MIN_INT64 - 1})

    with pytest.raises(ValidationError):
        ais.load({'prev': models.MAX_INT64 + 1})
