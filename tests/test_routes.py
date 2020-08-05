from urllib.parse import urljoin, urlparse
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
    return p.lock_or_create_creditor(2)


@pytest.fixture(scope='function')
def account(creditor):
    return p.create_account(2, 1)


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


def _get_log_entries(client, url):
    return _get_all_pages(client, url, page_type='LogEntriesPage', streaming=True)


def _get_creditor_accounts(client, url):
    return _get_all_pages(client, url, page_type='ObjectReferencesPage')


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

    entries = _get_log_entries(client, '/creditors/2/log')
    assert len(entries) == 0


def test_update_creditor(client, creditor):
    r = client.patch('/creditors/2222/', json={})
    assert r.status_code == 404

    r = client.patch('/creditors/2/', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'
    assert data['latestUpdateId'] == m.FIRST_LOG_ENTRY_ID
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['createdOn']

    entries = _get_log_entries(client, '/creditors/2/log')
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

    assert len(_get_log_entries(client, '/creditors/2/log')) == 0


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
    assert data['latestUpdateId'] < m.FIRST_LOG_ENTRY_ID
    assert iso8601.parse_date(data['latestUpdateAt'])

    # one page
    items = _get_creditor_accounts(client, '/creditors/2/accounts/')
    assert [item['uri'] for item in items] == ['1/']

    # add two more accounts
    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:9223372036854775809'})
    assert r.status_code == 201
    r = client.post('/creditors/2/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:9223372036854775808'})
    assert r.status_code == 201

    # two pages
    items = _get_creditor_accounts(client, '/creditors/2/accounts/')
    assert [item['uri'] for item in items] == ['9223372036854775808/', '9223372036854775809/', '1/']
    assert u64_to_i64(9223372036854775808) < u64_to_i64(9223372036854775809) < u64_to_i64(1)

    # check log entires
    entries = _get_log_entries(client, '/creditors/2/log')
    assert len(entries) == 3
    assert [e['entryId'] for e in entries] == [m.FIRST_LOG_ENTRY_ID, m.FIRST_LOG_ENTRY_ID + 1, m.FIRST_LOG_ENTRY_ID + 2]
    assert [e['previousEntryId'] for e in entries] == [1, m.FIRST_LOG_ENTRY_ID, m.FIRST_LOG_ENTRY_ID + 1]
    assert [e['objectType'] for e in entries] == 3 * ['Account']
    assert [e['object']['uri'] for e in entries] == [
        '/creditors/2/accounts/1/',
        '/creditors/2/accounts/9223372036854775809/',
        '/creditors/2/accounts/9223372036854775808/',
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
    assert data['latestUpdateId'] < m.FIRST_LOG_ENTRY_ID
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


def test_create_account(client, creditor):
    entries = _get_log_entries(client, '/creditors/2/log')
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
    createdAt = data1['createdAt']
    assert latestUpdateId == m.FIRST_LOG_ENTRY_ID
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
                'first': f'/creditors/2/accounts/1/entries?prev={latestUpdateId + 1}',
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

    entries = _get_log_entries(client, '/creditors/2/log')
    assert len(entries) == 1
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


def test_get_account(client, account):
    r = client.get('/creditors/2/accounts/1111/')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Account'
    assert data['uri'] == '/creditors/2/accounts/1/'
