import pytest
import math
from marshmallow import ValidationError
from datetime import date, datetime, timezone
from swpt_lib.utils import i64_to_u64
from swpt_creditors import schemas
from swpt_creditors import models
from swpt_creditors import procedures
from swpt_creditors.routes import context

D_ID = -1
C_ID = 1


def test_serialize_creditor(app):
    c = models.Creditor(
        creditor_id=C_ID,
        created_at_ts=datetime(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        last_log_entry_id=1,
        creditor_latest_update_id=1,
        creditor_latest_update_ts=datetime(2020, 1, 1),
    )
    cs = schemas.CreditorSchema(context=context)
    assert cs.dump(c) == {
        'type': 'Creditor',
        'uri': '/creditors/1/',
        'createdAt': '2019-11-30T00:00:00',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_deserialize_creditor(app):
    cs = schemas.CreditorSchema(context=context)

    data = cs.load({
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'Creditor',
        'latest_update_id': 2,
    }

    data = cs.load({
        'type': 'Creditor',
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'Creditor',
        'latest_update_id': 2,
    }

    with pytest.raises(ValidationError):
        cs.load({})

    with pytest.raises(ValidationError):
        cs.load({'type': 'WrongType', 'latestUpdateId': 2})


def test_serialize_wallet(app):
    c = models.Creditor(
        creditor_id=C_ID,
        created_at_ts=datetime(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        last_log_entry_id=12345,
    )
    ws = schemas.WalletSchema(context=context)
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
            'type': 'PaginatedStream',
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
        object_type='Account',
        object_uri='/creditors/1/accounts/123/',
        object_update_id=777,
        is_deleted=True,
        data=None,
    )
    les = schemas.LogEntrySchema(context=context)
    assert les.dump(le) == {
        'type': 'LogEntry',
        'entryId': 12345,
        'addedAt': '2020-01-02T00:00:00',
        'objectType': 'Account',
        'object': {'uri': '/creditors/1/accounts/123/'},
        'objectUpdateId': 777,
        'deleted': True,
    }

    le.is_deleted = False
    le.data = {'test': 'test', 'list': [1, 2, 3]}
    le.object_update_id = None
    assert les.dump(le) == {
        'type': 'LogEntry',
        'entryId': 12345,
        'addedAt': '2020-01-02T00:00:00',
        'objectType': 'Account',
        'object': {'uri': '/creditors/1/accounts/123/'},
        'deleted': False,
        'data': le.data,
    }


def test_serialize_log_entries_page(app):
    le = models.LogEntry(
        creditor_id=C_ID,
        entry_id=12345,
        added_at_ts=datetime(2020, 1, 2),
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
    les = schemas.LogEntrySchema(context=context)
    leps = schemas.LogEntriesPageSchema(context=context)
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
        created_at_ts=datetime(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        last_log_entry_id=1,
        account_list_latest_update_id=1,
        account_list_latest_update_ts=datetime(2020, 1, 1),
    )
    als = schemas.AccountListSchema(context=context)
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
        created_at_ts=datetime(2019, 11, 30),
        status=0,
        deactivated_at_date=None,
        last_log_entry_id=1,
        transfer_list_latest_update_id=1,
        transfer_list_latest_update_ts=datetime(2020, 1, 1),
    )
    tls = schemas.TransferListSchema(context=context)
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
        unit='XXX',
        hide=False,
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    ads = schemas.AccountDisplaySchema(context=context)
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': '/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'debtorName': 'Test Debtor',
        'unit': 'XXX',
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ad.debtor_name = None
    ad.unit = None
    assert ads.dump(ad) == {
        'type': 'AccountDisplay',
        'uri': '/creditors/1/accounts/18446744073709551615/display',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }


def test_deserialize_account_display(app):
    ads = schemas.AccountDisplaySchema(context=context)

    base_data = {
        'amountDivisor': 1.0,
        'decimalPlaces': 0,
        'hide': False,
        'latestUpdateId': 2,
    }

    data = ads.load(base_data)
    assert data == {
        'type': 'AccountDisplay',
        'amount_divisor': 1.0,
        'decimal_places': 0,
        'hide': False,
        'latest_update_id': 2,
    }

    data = ads.load({
        'type': 'AccountDisplay',
        'debtorName': 'Test Debtor',
        'unit': 'XXX',
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'hide': False,
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'AccountDisplay',
        'amount_divisor': 100.0,
        'decimal_places': 2,
        'hide': False,
        'optional_unit': 'XXX',
        'optional_debtor_name': 'Test Debtor',
        'latest_update_id': 2,
    }

    with pytest.raises(ValidationError):
        x = base_data.copy()
        x['type'] = 'WrongType'
        ads.load(x)

    with pytest.raises(ValidationError, match='Length must be between 1 and'):
        x = base_data.copy()
        x.update({'debtorName': 'Test Debtor', 'unit': 1000 * 'x'})
        ads.load(x)

    with pytest.raises(ValidationError, match='Length must be between 1 and'):
        x = base_data.copy()
        x.update({'debtorName': 1000 * 'x', 'unit': 'USD'})
        ads.load(x)

    with pytest.raises(ValidationError):
        x = base_data.copy()
        x['amountDivisor'] = 0.0
        ads.load(x)

    with pytest.raises(ValidationError):
        x = base_data.copy()
        x['amountDivisor'] = -0.01
        ads.load(x)

    with pytest.raises(ValidationError):
        x = base_data.copy()
        x['decimalPlaces'] = 10000
        ads.load(x)


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
    aes = schemas.AccountExchangeSchema(context=context)
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
    aes = schemas.AccountExchangeSchema(context=context)

    data = aes.load({
        'minPrincipal': -1000,
        'maxPrincipal': -500,
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'AccountExchange',
        'min_principal': -1000,
        'max_principal': -500,
        'latest_update_id': 2,
    }

    data = aes.load({
        'type': 'AccountExchange',
        'minPrincipal': 1000,
        'maxPrincipal': 5000,
        'policy': 'test policy',
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'AccountExchange',
        'min_principal': 1000,
        'max_principal': 5000,
        'optional_policy': 'test policy',
        'latest_update_id': 2,
    }

    with pytest.raises(ValidationError, match='Invalid type.'):
        aes.load({
            'type': 'WrongType',
            'minPrincipal': 1000,
            'maxPrincipal': 5000,
            'latestUpdateId': 2,
        })

    with pytest.raises(ValidationError, match='maxPrincipal must be equal or greater than minPrincipal.'):
        aes.load({
            'minPrincipal': 5000,
            'maxPrincipal': 1000,
            'latestUpdateId': 2,
        })

    with pytest.raises(ValidationError, match='greater than or equal'):
        aes.load({
            'minPrincipal': models.MIN_INT64 - 1,
            'maxPrincipal': 1000,
            'latestUpdateId': 2,
        })

    with pytest.raises(ValidationError, match='less than or equal'):
        aes.load({
            'minPrincipal': 0,
            'maxPrincipal': models.MAX_INT64 + 1,
            'latestUpdateId': 2,
        })

    with pytest.raises(ValidationError, match='Length must be between 1 and'):
        aes.load({
            'minPrincipal': 0,
            'maxPrincipal': 0,
            'policy': 1000 * 'x',
            'latestUpdateId': 2,
        })


def test_serialize_account_knowledge(app):
    ak = models.AccountKnowledge(
        creditor_id=C_ID,
        debtor_id=D_ID,
        data={
            'identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
            'debtorInfo': {
                'type': 'DebtorInfo',
                'url': 'http://example.com',
                'contentType': 'text/html',
                'sha256': 32 * '01',
            },
            'interestRate': 11.0,
            'interestRateChangedAt': '2020-01-02T00:00:00',

            # ignored
            'latestUpdateId': 1000,
            'latestUpdateAt': '2010-01-01T00:00:00',
            'account': '',
            'uri': '',
            'type': '',
        },
        latest_update_id=1,
        latest_update_ts=datetime(2020, 1, 1),
    )
    aks = schemas.AccountKnowledgeSchema(context=context)
    assert aks.dump(ak) == {
        'type': 'AccountKnowledge',
        'uri': '/creditors/1/accounts/18446744073709551615/knowledge',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
        'debtorInfo': {
            'type': 'DebtorInfo',
            'url': 'http://example.com',
            'contentType': 'text/html',
            'sha256': 32 * '01',
        },
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
    }

    ak.data = {
        'interestRate': 'not a number',
        'interestRateChangedAt': '2020-01-02T00:00:00',
        'debtorInfo': {
            'type': 'DebtorInfo',
            'url': 'http://example.com',
        },
    }
    assert aks.dump(ak) == {
        'type': 'AccountKnowledge',
        'uri': '/creditors/1/accounts/18446744073709551615/knowledge',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'interestRate': 'not a number',
        'interestRateChangedAt': '2020-01-02T00:00:00',
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
        'debtorInfo': {
            'type': 'DebtorInfo',
            'url': 'http://example.com',
        },
    }


def test_deserialize_account_knowledge(app):
    n = int(0.4 * schemas.AccountKnowledgeSchema.MAX_BYTES)
    aks = schemas.AccountKnowledgeSchema(context=context)

    data = aks.load({'latestUpdateId': 1})
    assert data == {
        'type': 'AccountKnowledge',
        'latest_update_id': 1,
        'data': {},
    }

    data = aks.load({
        'type': 'AccountKnowledge',
        'latestUpdateId': 1,
        'interest_rate_changed_at_ts': '1970-01-01T00:00:00Z',
        'unknownField': {'innerField': n * 'Ш'},
    })
    assert data == {
        'type': 'AccountKnowledge',
        'latest_update_id': 1,
        'data': {
            'interest_rate_changed_at_ts': '1970-01-01T00:00:00Z',
            'unknownField': {'innerField': n * 'Ш'},
        }
    }

    data = aks.load({
        'type': 'AccountKnowledge',
        'latestUpdateId': 1,
        'identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
        'debtorInfo': {
            'type': 'DebtorInfo',
            'url': 'http://example.com',
            'contentType': 'text/html',
            'sha256': 16 * 'BA01',
        },
        'interestRate': 11.0,
        'interestRateChangedAt': '1970-01-01T00:00:00Z',
    })
    assert data == {
        'type': 'AccountKnowledge',
        'latest_update_id': 1,
        'data': {
            'identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
            'debtorInfo': {
                'type': 'DebtorInfo',
                'url': 'http://example.com',
                'contentType': 'text/html',
                'sha256': 16 * 'BA01',
            },
            'interestRate': 11.0,
            'interestRateChangedAt': '1970-01-01T00:00:00Z',
        },
    }

    data = aks.load({
        'type': 'AccountKnowledge',
        'latestUpdateId': 1,
        'identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
        'debtorInfo': {
            'url': 'http://example.com',
        },
        'interestRate': 11.0,
        'interestRateChangedAt': '2020-01-02T00:00:00',
    })
    assert data == {
        'type': 'AccountKnowledge',
        'latest_update_id': 1,
        'data': {
            'identity': {'type': 'AccountIdentity', 'uri': 'https://example.com/USD/accounts/123'},
            'debtorInfo': {
                'url': 'http://example.com',
            },
            'interestRate': 11.0,
            'interestRateChangedAt': '2020-01-02T00:00:00',
        },
    }

    with pytest.raises(ValidationError, match='Invalid type.'):
        aks.load({'type': 'WrongType', 'latestUpdateId': 1})

    with pytest.raises(ValidationError, match='Longer than maximum length'):
        aks.load({'latestUpdateId': 1, 'identity': {'type': 'AccountIdentity', 'uri': 2 * n * 'x'}})

    with pytest.raises(ValidationError, match='Not a valid datetime.'):
        aks.load({'latestUpdateId': 1, 'interestRateChangedAt': 'INVALID TIMESTAMP'})

    with pytest.raises(ValidationError, match='Missing data for required field.'):
        aks.load({'latestUpdateId': 1, 'debtorInfo': {}})

    with pytest.raises(ValidationError, match='Not a valid number.'):
        aks.load({'latestUpdateId': 1, 'interestRate': 'not a number'})

    with pytest.raises(ValidationError, match=r'The total length of the stored data exceeds \d'):
        aks.load({'latestUpdateId': 1, 'tooLong': 3 * n * 'x'})

    with pytest.raises(ValidationError, match=r'The total length of the stored data exceeds \d'):
        d = {str(x): x for x in range(n)}
        d['latestUpdateId'] = 1
        aks.load(d)

    with pytest.raises(ValidationError, match='not JSON compliant'):
        aks.loads('{"latestUpdateId": 1, "notJsonCompliant": NaN}')

    for field in ['uri', 'account', 'latestUpdateAt']:
        with pytest.raises(ValidationError, match=f'Can not modify "{field}".'):
            aks.load({'latestUpdateId': 1, field: 'x'})


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
    acs = schemas.AccountConfigSchema(context=context)
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
    acs = schemas.AccountConfigSchema(context=context)

    data = acs.load({
        'negligibleAmount': 1.0,
        'scheduledForDeletion': True,
        'allowUnsafeDeletion': False,
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'AccountConfig',
        'negligible_amount': 1.0,
        'is_scheduled_for_deletion': True,
        'allow_unsafe_deletion': False,
        'latest_update_id': 2,
    }

    data = acs.load({
        'type': 'AccountConfig',
        'negligibleAmount': 1.0,
        'allowUnsafeDeletion': True,
        'scheduledForDeletion': False,
        'latestUpdateId': 2,
    })
    assert data == {
        'type': 'AccountConfig',
        'negligible_amount': 1.0,
        'is_scheduled_for_deletion': False,
        'allow_unsafe_deletion': True,
        'latest_update_id': 2,
    }

    with pytest.raises(ValidationError, match='Invalid type.'):
        acs.load({
            'type': 'WrongType',
            'negligibleAmount': 1.0,
            'allowUnsafeDeletion': True,
            'scheduledForDeletion': False,
            'latestUpdateId': 2,
        })

    with pytest.raises(ValidationError, match='Must be greater than or equal to 1 and'):
        acs.load({
            'negligibleAmount': 1.0,
            'allowUnsafeDeletion': True,
            'scheduledForDeletion': False,
            'latestUpdateId': 0,
        })

    with pytest.raises(ValidationError, match='Must be greater than or equal to 1 and'):
        acs.load({
            'negligibleAmount': 1.0,
            'allowUnsafeDeletion': True,
            'scheduledForDeletion': False,
            'latestUpdateId': models.MAX_INT64 + 1,
        })


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
        status_flags=0,
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
    ais = schemas.AccountInfoSchema(context=context)
    assert ais.dump(ad) == {
        'type': 'AccountInfo',
        'uri': '/creditors/1/accounts/18446744073709551615/info',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'interestRate': 7.0,
        'interestRateChangedAt': '2000-01-01T00:00:00',
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
        'safeToDelete': True,
        'latestUpdateId': 1,
        'latestUpdateAt': '2020-01-01T00:00:00',
        'identity': {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/!bm90IFVSTCBzYWZl'},
        'configError': 'TEST_ERROR',
        'debtorInfo': {'type': 'DebtorInfo', 'url': 'https://example.com/debtor'},
    }


def test_serialize_account(db_session):
    procedures.create_new_creditor(C_ID)
    procedures.activate_creditor(C_ID)
    procedures.create_new_account(C_ID, D_ID)
    account = models.Account.get_instance((C_ID, D_ID))
    account_schema = schemas.AccountSchema(context=context)
    ads = schemas.AccountDisplaySchema(context=context)
    acs = schemas.AccountConfigSchema(context=context)
    ais = schemas.AccountInfoSchema(context=context)
    als = schemas.AccountLedgerSchema(context=context)
    aes = schemas.AccountExchangeSchema(context=context)
    aks = schemas.AccountKnowledgeSchema(context=context)
    assert account_schema.dump(account) == {
        'type': 'Account',
        'uri': '/creditors/1/accounts/18446744073709551615/',
        'accountList': {'uri': '/creditors/1/account-list'},
        'createdAt': account.created_at_ts.isoformat(),
        'latestUpdateId': account.latest_update_id,
        'latestUpdateAt': account.latest_update_ts.isoformat(),
        'debtor': {'type': 'DebtorIdentity', 'uri': 'swpt:18446744073709551615'},
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
        'exchange_rate': 2.5,
        'account': {'uri': '/creditors/2/accounts/1/'}
    }
    cps = schemas.CurrencyPegSchema()
    assert cps.dump(cp) == {
        'type': 'CurrencyPeg',
        'exchangeRate': 2.5,
        'account': {'uri': '/creditors/2/accounts/1/'}
    }

    del cp['type']
    assert cps.dump(cp) == {
        'type': 'CurrencyPeg',
        'exchangeRate': 2.5,
        'account': {'uri': '/creditors/2/accounts/1/'}
    }


def test_deserialize_currency_peg(app):
    cps = schemas.CurrencyPegSchema()

    data = cps.load({
        'account': {'uri': '/creditors/2/accounts/1/'},
        'exchangeRate': 2.5,
    })
    assert data == {
        'type': 'CurrencyPeg',
        'exchange_rate': 2.5,
        'account': {'uri': '/creditors/2/accounts/1/'},
    }

    data = cps.load({
        'type': 'CurrencyPeg',
        'exchangeRate': 2.5,
        'account': {'uri': '/creditors/2/accounts/1/'},
    })
    assert data == {
        'type': 'CurrencyPeg',
        'exchange_rate': 2.5,
        'account': {'uri': '/creditors/2/accounts/1/'},
    }

    with pytest.raises(ValidationError, match='Invalid type.'):
        cps.load({
            'type': 'WrongType',
            'exchangeRate': 2.5,
            'account': {'uri': '/creditors/2/accounts/1/'},
        })

    with pytest.raises(ValidationError, match='Missing data for required field.'):
        cps.load({'exchangeRate': 2.5})

    with pytest.raises(ValidationError, match='Missing data for required field.'):
        cps.load({'account': {'uri': '/creditors/2/accounts/1/'}})

    with pytest.raises(ValidationError, match='Missing data for required field.'):
        cps.load({'exchangeRate': 2.5, 'account': {}})

    with pytest.raises(ValidationError, match='Must be greater than or equal to 0'):
        cps.load({
            'type': 'CurrencyPeg',
            'exchangeRate': -0.01,
            'account': {'uri': '/creditors/2/accounts/1/'},
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
        status_flags=0,
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
        ledger_last_entry_id=0,
    )
    als = schemas.AccountLedgerSchema(context=context)
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
        'nextEntryId': 1,
        'latestUpdateId': 2,
        'latestUpdateAt': '2020-01-02T00:00:00',
    }

    ad.ledger_last_entry_id = 54321
    assert als.dump(ad)['nextEntryId'] == 54322
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
    )
    les = schemas.LedgerEntrySchema(context=context)
    assert les.dump(le) == {
        'type': 'LedgerEntry',
        'ledger': {'uri': '/creditors/1/accounts/18446744073709551615/ledger'},
        'entryId': 2,
        'principal': 3000,
        'transfer': {'uri': '/creditors/1/accounts/18446744073709551615/transfers/4-666'},
        'aquiredAmount': 1000,
        'addedAt': '2020-01-02T00:00:00',
    }

    le.creation_date = None
    assert les.dump(le) == {
        'type': 'LedgerEntry',
        'ledger': {'uri': '/creditors/1/accounts/18446744073709551615/ledger'},
        'entryId': 2,
        'principal': 3000,
        'aquiredAmount': 1000,
        'addedAt': '2020-01-02T00:00:00',
    }

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
    }
    pls = schemas.PaginatedListSchema(context=context)
    assert pls.dump(pl) == {
        'type': 'PaginatedList',
        'itemsType': 'String',
        'first': '/first',
    }


def test_serialize_paginated_stream(app):
    ps = {
        'items_type': 'String',
        'first': '/first',
        'forthcoming': '/more',
    }
    pss = schemas.PaginatedStreamSchema(context=context)
    assert pss.dump(ps) == {
        'type': 'PaginatedStream',
        'itemsType': 'String',
        'first': '/first',
        'forthcoming': '/more',
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
    )
    lep = {
        'uri': '/test',
        'items': [le],
        'next': '?prev=1',
    }
    les = schemas.LedgerEntrySchema(context=context)
    leps = schemas.LedgerEntriesPageSchema(context=context)
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
    orps = schemas.ObjectReferencesPageSchema(context=context)
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
    dis = schemas.DebtorIdentitySchema(context=context)
    assert dis.dump(di) == {
        'type': 'DebtorIdentity',
        'uri': 'swpt:1',
    }


def test_deserialize_debtor_identity(app):
    dis = schemas.DebtorIdentitySchema(context=context)

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
    ais = schemas.AccountIdentitySchema(context=context)
    assert ais.dump(ai) == {
        'type': 'AccountIdentity',
        'uri': 'swpt:1/2',
    }


def test_deserialize_account_identity(app):
    ais = schemas.AccountIdentitySchema(context=context)

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
    pps = schemas.PaginationParametersSchema(context=context)

    data = pps.load({})
    assert data == {}

    data = pps.load({'prev': 'p', 'stop': 's'})
    assert data == {'prev': 'p', 'stop': 's'}


def test_deserialize_creditor_creation_request(app):
    ccr = schemas.CreditorCreationRequestSchema(context=context)
    assert ccr.load({}) == {'type': 'CreditorCreationRequest', 'activate': False}
    assert ccr.load({
        'type': 'CreditorCreationRequest',
        'activate': True
    }) == {
        'type': 'CreditorCreationRequest',
        'activate': True,
    }

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
        sender_id='1',
        recipient_id='1111',
    )
    cts = schemas.CommittedTransferSchema(context=context)
    assert cts.dump(ct) == {
        'type': 'CommittedTransfer',
        'uri': '/creditors/1/accounts/18446744073709551615/transfers/4-666',
        'account': {'uri': '/creditors/1/accounts/18446744073709551615/'},
        'committedAt': '2020-01-01T00:00:00',
        'sender': {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/1'},
        'recipient': {'type': 'AccountIdentity', 'uri': 'swpt:18446744073709551615/1111'},
        'acquiredAmount': 1000,
        'note': {"test": "test", "list": [1, 2, 3]},
    }

    ct.transfer_note = ''
    ct.coordinator_type = 'interest'
    data = cts.dump(ct)
    assert data['note'] == {}
    assert data['coordinator'] == 'interest'

    ct.transfer_note = 'test'
    assert cts.dump(ct)['note'] == {'type': 'TextMessage', 'content': 'test'}

    ct.transfer_note = '[]'
    assert cts.dump(ct)['note'] == {'type': 'TextMessage', 'content': '[]'}

    # invalid identity
    ct.sender_id = 1000 * '1'
    ct.recipient_id = 1000 * '1'
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
    assert ais.load({'prev': str(i64_to_u64(0))}) == {'prev': str(i64_to_u64(0))}
    assert ais.load({'prev': str(i64_to_u64(-1))}) == {'prev': str(i64_to_u64(-1))}
    assert ais.load({'prev': str(i64_to_u64(1))}) == {'prev': str(i64_to_u64(1))}
    assert ais.load({'prev': str(i64_to_u64(models.MIN_INT64))}) == {'prev': str(i64_to_u64(models.MIN_INT64))}
    assert ais.load({'prev': str(i64_to_u64(models.MAX_INT64))}) == {'prev': str(i64_to_u64(models.MAX_INT64))}

    with pytest.raises(ValidationError):
        ais.load({'prev': ''})

    with pytest.raises(ValidationError):
        ais.load({'prev': 65 * 'x'})

    with pytest.raises(ValidationError):
        ais.load({'prev': '?s^#@'})


def test_serialize_debtor_info(app):
    dis = schemas.DebtorInfoSchema()

    assert dis.dump({
        'url': 'http://example.com',
    }) == {
        'type': 'DebtorInfo',
        'url': 'http://example.com',
    }

    assert dis.dump({
        'url': 'http://example.com',
        'optional_content_type': 'text/html',
        'optional_sha256': 16 * 'BA01',
    }) == {
        'type': 'DebtorInfo',
        'url': 'http://example.com',
        'contentType': 'text/html',
        'sha256': 16 * 'BA01',
    }


def test_deserialize_debtor_info(app):
    dis = schemas.DebtorInfoSchema()

    data = dis.load({
        'url': 'http://example.com',
    })
    assert data == {
        'type': 'DebtorInfo',
        'url': 'http://example.com',
    }

    data = dis.load({
        'type': 'DebtorInfo',
        'url': 'http://example.com',
        'contentType': 'text/html',
        'sha256': 16 * 'BA01',
    })
    assert data == {
        'type': 'DebtorInfo',
        'url': 'http://example.com',
        'optional_content_type': 'text/html',
        'optional_sha256': 16 * 'BA01',
    }

    with pytest.raises(ValidationError):
        dis.load({'type': 'WrongType'})

    with pytest.raises(ValidationError):
        dis.load({'url': 1000 * 'x'})

    with pytest.raises(ValidationError):
        dis.load({'url': 'http://example.com', 'content_type': 1000 * 'x'})

    with pytest.raises(ValidationError):
        dis.load({'url': 'http://example.com', 'sha256': 64 * 'G'})

    with pytest.raises(ValidationError):
        dis.load({'url': 'http://example.com', 'sha256': 64 * 'f'})
