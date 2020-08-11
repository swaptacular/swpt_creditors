from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
import pytest
import iso8601
from swpt_lib.utils import u64_to_i64
from swpt_creditors import models as m
from swpt_creditors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def creditor(db_session):
    return p.create_new_creditor(2)


@pytest.fixture(scope='function')
def account(creditor):
    return p.create_new_account(2, 1)


def _get_all_pages(client, url, page_type, streaming=False):
    r = client.get(url)
    assert r.status_code == 200

    data = r.get_json()
    assert data['type'] == page_type
    assert urlparse(data['uri']) == urlparse(url)
    if streaming:
        assert 'next' in data or 'forthcoming' in data
        assert 'next' not in data or 'forthcoming' not in data
    else:
        assert 'forthcoming' not in data

    items = data['items']
    assert isinstance(items, list)

    if 'next' in data:
        items.extend(_get_all_pages(client, urljoin(url, data['next']), page_type, streaming))

    return items


def test_create_creditor(client):
    r = client.get('/creditors/2/')
    assert r.status_code == 404

    r = client.post('/creditors/2/', json={})
    assert r.status_code == 201
    assert r.headers['Location'] == 'http://example.com/creditors/2/'
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['createdOn'])

    r = client.post('/creditors/2/', json={})
    assert r.status_code == 409

    r = client.get('/creditors/2/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['createdOn'])

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 0


def test_update_creditor(client, creditor):
    r = client.patch('/creditors/2222/', json={})
    assert r.status_code == 404

    r = client.patch('/creditors/2/', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'
    assert data['latestUpdateId'] == 2
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['createdOn']

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 1
    e = entries[0]
    assert e['type'] == 'LogEntry'
    assert e['entryId'] == m.FIRST_LOG_ENTRY_ID
    assert e['objectType'] == 'Creditor'
    assert e['object'] == {'uri': '/creditors/2/'}
    assert not e.get('deleted')
    assert iso8601.parse_date(e['addedAt'])


def test_get_wallet(client, creditor):
    r = client.get('/creditors/2222/wallet')
    assert r.status_code == 404

    r = client.get('/creditors/2/wallet')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Wallet'
    assert data['uri'] == '/creditors/2/wallet'
    assert data['creditor'] == {'uri': '/creditors/2/'}
    log = data['log']
    assert log['type'] == 'PaginatedList'
    assert log['first'] == '/creditors/2/log'
    assert log['forthcoming'] == '/creditors/2/log?prev=1'
    assert log['itemsType'] == 'LogEntry'
    dt = data['transferList']
    assert dt['uri'] == '/creditors/2/transfer-list'
    ar = data['accountList']
    assert ar['uri'] == '/creditors/2/account-list'


def test_get_log_page(client, creditor):
    r = client.get('/creditors/2222/log')
    assert r.status_code == 404

    r = client.get('/creditors/2/log')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'LogEntriesPage'
    assert data['items'] == []
    assert data['forthcoming'] == '?prev=1'
    assert 'next' not in data


def test_account_list_page(client, account):
    r = client.get('/creditors/2222/account-list')
    assert r.status_code == 404

    r = client.get('/creditors/2/account-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountList'
    assert data['uri'] == '/creditors/2/account-list'
    assert data['wallet'] == {'uri': '/creditors/2/wallet'}
    assert data['first'] == '/creditors/2/accounts/'
    assert data['itemsType'] == 'ObjectReference'
    assert data['latestUpdateId'] > 1
    assert iso8601.parse_date(data['latestUpdateAt'])

    # one account (one page)
    items = _get_all_pages(client, '/creditors/2/accounts/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == ['1/']

    # add two more accounts
    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:9223372036854775809'})
    assert r.status_code == 201
    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:9223372036854775808'})
    assert r.status_code == 201

    # three accounts (two pages)
    items = _get_all_pages(client, '/creditors/2/accounts/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == ['9223372036854775808/', '9223372036854775809/', '1/']
    assert u64_to_i64(9223372036854775808) < u64_to_i64(9223372036854775809) < u64_to_i64(1)

    # check log entires
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 6
    assert [(e['objectType'], e['object']['uri']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/'),
        ('AccountList', '/creditors/2/account-list'),
        ('Account', '/creditors/2/accounts/9223372036854775809/'),
        ('AccountList', '/creditors/2/account-list'),
        ('Account', '/creditors/2/accounts/9223372036854775808/'),
        ('AccountList', '/creditors/2/account-list'),
    ]
    assert all(['deleted' not in e for e in entries])
    assert all(['data' not in e for e in entries])
    assert all([e['type'] == 'LogEntry' not in e for e in entries])
    assert all([iso8601.parse_date(e['addedAt']) not in e for e in entries])


def test_transfer_list_page(client, creditor):
    r = client.get('/creditors/2222/transfer-list')
    assert r.status_code == 404

    r = client.get('/creditors/2/transfer-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'TransferList'
    assert data['uri'] == '/creditors/2/transfer-list'
    assert data['wallet'] == {'uri': '/creditors/2/wallet'}
    assert data['first'] == '/creditors/2/transfers/'
    assert data['itemsType'] == 'ObjectReference'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])


def test_account_lookup(client, creditor):
    r = client.post('/creditors/2/account-lookup', json={'type': 'AccountIdentity', 'uri': 'xxx:1/2'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/2/account-lookup', json={'type': 'AccountIdentity', 'uri': 'swpt:1/2'})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorIdentity'
    assert data['uri'] == 'swpt:1'


def test_debtor_lookup(client, account):
    r = client.post('/creditors/2/debtor-lookup', json={'type': 'DebtorIdentity', 'uri': 'xxx:1'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/2/debtor-lookup', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://example.com/creditors/2/accounts/1/'

    r = client.post('/creditors/2/debtor-lookup', json={'type': 'DebtorIdentity', 'uri': 'swpt:1111'})
    assert r.status_code == 204
    assert r.data == b''


def test_create_account(client, creditor):
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 0

    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'xxx:1'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 201
    data1 = r.get_json()
    assert r.headers['Location'] == 'http://example.com/creditors/2/accounts/1/'
    latestUpdateId = data1['latestUpdateId']
    latestUpdateAt = data1['latestUpdateAt']
    ledgerLatestEntryId = data1['ledger'].get('latestEntryId', 0)
    createdAt = data1['createdAt']
    assert latestUpdateId == 1
    assert iso8601.parse_date(latestUpdateAt)
    assert iso8601.parse_date(createdAt)
    assert data1 == {
        'type': 'Account',
        'uri': '/creditors/2/accounts/1/',
        'accountList': {'uri': '/creditors/2/account-list'},
        'createdAt': createdAt,
        'debtorIdentity': {
            'type': 'DebtorIdentity',
            'uri': 'swpt:1',
        },
        'config': {
            'type': 'AccountConfig',
            'uri': '/creditors/2/accounts/1/config',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'allowUnsafeDeletion': False,
            'negligibleAmount': 1e+30,
            'scheduledForDeletion': False,
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'display': {
            'type': 'AccountDisplay',
            'uri': '/creditors/2/accounts/1/display',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'amountDivisor': 1.0,
            'decimalPlaces': 0,
            'hide': False,
            'ownUnitPreference': 0,
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'exchange': {
            'type': 'AccountExchange',
            'uri': '/creditors/2/accounts/1/exchange',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'minPrincipal': -9223372036854775808,
            'maxPrincipal': 9223372036854775807,
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'info': {
            'type': 'AccountInfo',
            'uri': '/creditors/2/accounts/1/info',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'interestRate': 0.0,
            'interestRateChangedAt': '1970-01-01T00:00:00+00:00',
            'overflown': False,
            'safeToDelete': False,
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'knowledge': {
            'type': 'AccountKnowledge',
            'uri': '/creditors/2/accounts/1/knowledge',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'interestRate': 0.0,
            'interestRateChangedAt': '1970-01-01T00:00:00+00:00',
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'ledger': {
            'type': 'AccountLedger',
            'uri': '/creditors/2/accounts/1/ledger',
            'account': {'uri': '/creditors/2/accounts/1/'},
            'principal': 0,
            'interest': 0,
            'entries': {
                'first': f'/creditors/2/accounts/1/entries?prev={ledgerLatestEntryId + 1}',
                'itemsType': 'LedgerEntry',
                'type': 'PaginatedList',
            },
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'latestUpdateAt': latestUpdateAt,
        'latestUpdateId': latestUpdateId,
    }

    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://example.com/creditors/2/accounts/1/'

    r = client.post('/creditors/2222/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 404

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 2
    assert entries[1]['objectType'] == 'AccountList'
    e = entries[0]
    assert e['type'] == 'LogEntry'
    assert e['entryId'] == m.FIRST_LOG_ENTRY_ID
    assert e['objectType'] == 'Account'
    assert e['object'] == {'uri': '/creditors/2/accounts/1/'}
    assert not e.get('deleted')
    assert iso8601.parse_date(e['addedAt'])

    r = client.get('/creditors/2/accounts/1/')
    assert r.status_code == 200
    data2 = r.get_json()
    assert data1 == data2

    for i in range(2, 11):
        r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': f'swpt:{i}'})
        assert r.status_code == 201
    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:11'})
    assert r.status_code == 403

    r = client.patch('/creditors/2/accounts/10/config', json={
        'scheduledForDeletion': True,
        'negligibleAmount': m.DEFAULT_NEGLIGIBLE_AMOUNT,
        'allowUnsafeDeletion': True,
    })
    assert r.status_code == 200
    r = client.delete('/creditors/2/accounts/10/')
    assert r.status_code == 204
    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:10'})
    assert r.status_code == 201


def test_get_account(client, account):
    r = client.get('/creditors/2/accounts/1111/')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Account'
    assert data['uri'] == '/creditors/2/accounts/1/'


def test_delete_account(client, account):
    r = client.delete('/creditors/2/accounts/1111/')
    assert r.status_code == 204

    r = client.delete('/creditors/2222/accounts/1/')
    assert r.status_code == 204

    r = client.delete('/creditors/2/accounts/1/')
    assert r.status_code == 403

    r = client.patch('/creditors/2/accounts/1/config', json={
        'scheduledForDeletion': True,
        'negligibleAmount': m.DEFAULT_NEGLIGIBLE_AMOUNT,
        'allowUnsafeDeletion': True,
    })
    assert r.status_code == 200

    r = client.get('/creditors/2/account-list')
    assert r.status_code == 200
    data = r.get_json()
    latest_update_id = data['latestUpdateId']
    latest_update_at = iso8601.parse_date(data['latestUpdateAt'])

    r = client.delete('/creditors/2/accounts/1/')
    assert r.status_code == 204

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 11
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('deleted', False))
            for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1, False),
        ('AccountList', '/creditors/2/account-list', 2, False),
        ('AccountConfig', '/creditors/2/accounts/1/config', 2, False),
        ('AccountList', '/creditors/2/account-list', 3, False),
        ('Account', '/creditors/2/accounts/1/', None, True),
        ('AccountConfig', '/creditors/2/accounts/1/config', None, True),
        ('AccountInfo', '/creditors/2/accounts/1/info', None, True),
        ('AccountLedger', '/creditors/2/accounts/1/ledger', None, True),
        ('AccountDisplay', '/creditors/2/accounts/1/display', None, True),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', None, True),
        ('AccountKnowledge', '/creditors/2/accounts/1/knowledge', None, True),
    ]

    r = client.get('/creditors/2/account-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['latestUpdateId'] == latest_update_id + 1
    assert iso8601.parse_date(data['latestUpdateAt']) >= latest_update_at


def test_account_config(client, account):
    r = client.get('/creditors/2/accounts/1111/config')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/config')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountConfig'
    assert data['uri'] == '/creditors/2/accounts/1/config'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['scheduledForDeletion'] is False
    assert data['allowUnsafeDeletion'] is False
    assert data['negligibleAmount'] == m.DEFAULT_NEGLIGIBLE_AMOUNT
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}

    request_data = {
        'negligibleAmount': 100.0,
        'allowUnsafeDeletion': True,
        'scheduledForDeletion': True,
    }

    r = client.patch('/creditors/2/accounts/1111/config', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/config', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountConfig'
    assert data['uri'] == '/creditors/2/accounts/1/config'
    assert data['latestUpdateId'] == 2
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['scheduledForDeletion'] is True
    assert data['allowUnsafeDeletion'] is True
    assert data['negligibleAmount'] == 100.0
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 3
    assert [(e['objectType'], e['object']['uri']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/'),
        ('AccountList', '/creditors/2/account-list'),
        ('AccountConfig', '/creditors/2/accounts/1/config'),
    ]


def test_account_display(client, account):
    r = client.get('/creditors/2/accounts/1111/display')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/display')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountDisplay'
    assert data['uri'] == '/creditors/2/accounts/1/display'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['ownUnitPreference'] == 0
    assert data['amountDivisor'] == 1.0
    assert data['hide'] is False
    assert data['decimalPlaces'] == 0
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'peg' not in data
    assert 'ownUnit' not in data
    assert 'debtorName' not in data

    r = client.post('/creditors/2/accounts/', json={'uri': 'swpt:11'})
    assert r.status_code == 201

    r = client.patch('/creditors/2/accounts/11/config', json={
        'scheduledForDeletion': True,
        'negligibleAmount': m.DEFAULT_NEGLIGIBLE_AMOUNT,
        'allowUnsafeDeletion': True,
    })
    assert r.status_code == 200

    r = client.patch('/creditors/2/accounts/11/display', json={
        'debtorName': 'existing debtor',
        'ownUnit': 'EUR',
    })
    assert r.status_code == 200

    request_data = {
        'type': 'AccountDisplay',
        'debtorName': 'United States of America',
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'ownUnit': 'USD',
        'ownUnitPreference': 1000000,
        'hide': True,
        'peg': {
            'type': 'CurrencyPeg',
            'exchangeRate': 10.0,
            'debtorIdentity': {
                'type': 'DebtorIdentity',
                'uri': 'swpt:11',
            },
            'debtorHomeUrl': 'https://example.com/debtor-home-url',
        },
    }

    r = client.patch('/creditors/2/accounts/1111/display', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountDisplay'
    assert data['uri'] == '/creditors/2/accounts/1/display'
    assert data['latestUpdateId'] == 2
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['debtorName'] == 'United States of America'
    assert data['amountDivisor'] == 100.0
    assert data['decimalPlaces'] == 2
    assert data['ownUnit'] == 'USD'
    assert data['ownUnitPreference'] == 1000000
    assert data['hide'] is True
    assert data['peg'] == {
        'type': 'CurrencyPeg',
        'exchangeRate': 10.0,
        'debtorIdentity': {
            'type': 'DebtorIdentity',
            'uri': 'swpt:11',
        },
        'display': {'uri': '/creditors/2/accounts/11/display'},
        'debtorHomeUrl': 'https://example.com/debtor-home-url',
    }

    request_data['peg']['debtorIdentity']['uri'] = 'INVALID'
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['peg']['debtorIdentity']['uri'] == ['The URI can not be recognized.']

    r = client.delete('/creditors/2/accounts/11/')
    assert r.status_code == 409

    request_data['peg']['debtorIdentity']['uri'] = 'swpt:1111'
    request_data['peg']['debtorHomeUrl'] = 'https://example.com/another-debtor-home-url'
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountDisplay'
    assert data['uri'] == '/creditors/2/accounts/1/display'
    assert data['debtorName'] == 'United States of America'
    assert data['amountDivisor'] == 100.0
    assert data['decimalPlaces'] == 2
    assert data['ownUnit'] == 'USD'
    assert data['ownUnitPreference'] == 1000000
    assert data['hide'] is True
    assert data['peg'] == {
        'type': 'CurrencyPeg',
        'exchangeRate': 10.0,
        'debtorIdentity': {
            'type': 'DebtorIdentity',
            'uri': 'swpt:1111',
        },
        'debtorHomeUrl': 'https://example.com/another-debtor-home-url',
    }

    request_data['debtorName'] = 'existing debtor'
    request_data['ownUnit'] = 'USD'
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 409
    data = r.get_json()
    assert data['errors']['json']['debtorName'] == ['Another account with this debtorName already exist.']

    request_data['debtorName'] = 'United States of America'
    request_data['ownUnit'] = 'EUR'
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 409
    data = r.get_json()
    assert data['errors']['json']['ownUnit'] == ['Another account with this ownUnit already exist.']

    r = client.post('/creditors/2/accounts/', json={'uri': 'swpt:1111'})
    assert r.status_code == 201

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 11
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountList', '/creditors/2/account-list', 2),
        ('Account', '/creditors/2/accounts/11/', 1),
        ('AccountList', '/creditors/2/account-list', 3),
        ('AccountConfig', '/creditors/2/accounts/11/config', 2),
        ('AccountDisplay', '/creditors/2/accounts/11/display', 2),
        ('AccountDisplay', '/creditors/2/accounts/1/display', 2),
        ('AccountDisplay', '/creditors/2/accounts/1/display', 3),
        ('Account', '/creditors/2/accounts/1111/', 1),
        ('AccountList', '/creditors/2/account-list', 4),
        ('AccountDisplay', '/creditors/2/accounts/1/display', 4),
    ]
    assert all([entries[i]['previousEntryId'] == entries[i - 1]['entryId']for i in range(1, len(entries))])
    assert all([entries[i]['entryId'] > entries[i - 1]['entryId']for i in range(1, len(entries))])

    r = client.delete('/creditors/2/accounts/11/')
    assert r.status_code == 204

    request_data['debtorName'] = 'existing debtor'
    request_data['ownUnit'] = 'EUR'
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 200


def test_account_exchange(client, account):
    r = client.get('/creditors/2/accounts/1111/exchange')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/exchange')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountExchange'
    assert data['uri'] == '/creditors/2/accounts/1/exchange'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['minPrincipal'] == p.MIN_INT64
    assert data['maxPrincipal'] == p.MAX_INT64
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'policy' not in data

    request_data = {
        'minPrincipal': 1000,
        'maxPrincipal': 2000,
    }

    r = client.patch('/creditors/2/accounts/1111/exchange', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountExchange'
    assert data['uri'] == '/creditors/2/accounts/1/exchange'
    assert data['latestUpdateId'] == 2
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['minPrincipal'] == 1000
    assert data['maxPrincipal'] == 2000
    assert 'policy' not in data

    r = client.patch('/creditors/2/accounts/1/exchange', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountExchange'
    assert data['latestUpdateId'] == 3
    assert data['minPrincipal'] == p.MIN_INT64
    assert data['maxPrincipal'] == p.MAX_INT64
    assert 'policy' not in data

    r = client.patch('/creditors/2/accounts/1/exchange', json={'policy': 'INVALID'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['policy'] == ['Invalid policy name.']

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 4
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountList', '/creditors/2/account-list', 2),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', 2),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', 3),
    ]


def test_account_knowledge(client, account):
    r = client.get('/creditors/2/accounts/1111/knowledge')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/knowledge')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/2/accounts/1/knowledge'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['interestRateChangedAt']) == m.TS0
    assert data['interestRate'] == 0.0
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'debtorInfoSha256' not in data
    assert 'accountIdentity' not in data

    request_data = {
        'debtorInfoSha256': 64 * '0',
        'interestRate': 11.5,
        'interestRateChangedAt': '2020-01-01T00:00:00Z',
        'accountIdentity': {
            'type': 'AccountIdentity',
            'uri': 'swpt:1/2',
        },
    }

    r = client.patch('/creditors/2/accounts/1111/knowledge', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/2/accounts/1/knowledge'
    assert data['latestUpdateId'] == 2
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['debtorInfoSha256'] == 64 * '0'
    assert data['interestRate'] == 11.5
    assert iso8601.parse_date(data['interestRateChangedAt']) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert data['accountIdentity'] == {'type': 'AccountIdentity', 'uri': 'swpt:1/2'}

    del request_data['debtorInfoSha256']
    del request_data['accountIdentity']
    r = client.patch('/creditors/2/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/2/accounts/1/knowledge'
    assert data['interestRate'] == 11.5
    assert data['latestUpdateId'] == 3
    assert iso8601.parse_date(data['interestRateChangedAt']) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert 'debtorInfoSha256' not in data
    assert 'accountIdentity' not in data

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 4
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountList', '/creditors/2/account-list', 2),
        ('AccountKnowledge', '/creditors/2/accounts/1/knowledge', 2),
        ('AccountKnowledge', '/creditors/2/accounts/1/knowledge', 3),
    ]


def test_get_account_info(client, account):
    r = client.get('/creditors/2/accounts/1111/info')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/info')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountInfo'
    assert data['uri'] == '/creditors/2/accounts/1/info'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['interestRateChangedAt']) == m.TS0
    assert data['interestRate'] == 0.0
    assert data['safeToDelete'] is False
    assert data['overflown'] is False
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'debtorInfoUrl' not in data
    assert 'accountIdentity' not in data
    assert 'configError' not in data


def test_get_account_ledger(client, account):
    r = client.get('/creditors/2/accounts/1111/ledger')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/ledger')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountLedger'
    assert data['uri'] == '/creditors/2/accounts/1/ledger'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['principal'] == 0
    assert data['interest'] == 0
    assert 'latestEntryId' not in data
    assert data['entries'] == {
        'itemsType': 'LedgerEntry',
        'type': 'PaginatedList',
        'first': '/creditors/2/accounts/1/entries?prev=1'
    }
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
