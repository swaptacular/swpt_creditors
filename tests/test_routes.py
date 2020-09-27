from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone, timedelta, date
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
    creditor = p.reserve_creditor(2)
    p.activate_creditor(2, creditor.reservation_id)
    return creditor


@pytest.fixture(scope='function')
def account(creditor):
    return p.create_new_account(2, 1)


@pytest.fixture(scope='function')
def ledger_entries(db_session, account, current_ts):
    from swpt_creditors.procedures.account_updates import _update_ledger

    data = m.AccountData.query.one()
    db_session.add(_update_ledger(data, 1, 100, 100, current_ts))
    db_session.add(_update_ledger(data, 2, 200, 350, current_ts))
    db_session.commit()
    p.process_pending_log_entries(2)


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


def test_auto_genereate_creditor_id(client):
    r = client.post('/creditor-reserve', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CreditorReservation'
    assert isinstance(data['creditorId'], str)
    assert isinstance(data['reservationId'], int)
    assert iso8601.parse_date(data['validUntil'])
    assert iso8601.parse_date(data['createdAt'])


def test_create_creditor(client):
    r = client.get('/creditors/2/')
    assert r.status_code == 403

    r = client.post('/creditors/2/reserve', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CreditorReservation'
    assert data['creditorId'] == '2'
    assert isinstance(data['reservationId'], int)
    assert iso8601.parse_date(data['validUntil'])
    assert iso8601.parse_date(data['createdAt'])
    reservation_id = data['reservationId']

    r = client.post('/creditors/2/reserve', json={})
    assert r.status_code == 409

    r = client.get('/creditors/2/')
    assert r.status_code == 403

    r = client.post('/creditors/2/activate', json={
        'reservationId': 123,
    })
    assert r.status_code == 422
    assert 'reservationId' in r.get_json()['errors']['json']

    r = client.post('/creditors/2/activate', json={
        'reservationId': reservation_id,
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['createdAt'])

    r = client.post('/creditors/2/activate', json={
        'reservationId': reservation_id,
    })
    assert r.status_code == 200

    r = client.post('/creditors/3/activate', json={
        'reservationId': 123,
    })
    assert r.status_code == 422
    assert 'reservationId' in r.get_json()['errors']['json']

    r = client.post('/creditors/3/activate', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/3/'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['createdAt'])

    r = client.post('/creditors/3/activate', json={})
    assert r.status_code == 409

    r = client.get('/creditors/2/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert iso8601.parse_date(data['createdAt'])

    p.process_pending_log_entries(2)
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 0

    r = client.get('/creditors/3/')
    assert r.status_code == 200

    r = client.post('/creditors/3/deactivate', json={})
    assert r.status_code == 204

    r = client.get('/creditors/3/')
    assert r.status_code == 403

    r = client.post('/creditors/3/deactivate', json={})
    assert r.status_code == 204


def test_get_creditors_list(client):
    r = client.post('/creditors/1/reserve', json={})
    assert r.status_code == 200
    r = client.post('/creditors/2/activate', json={})
    assert r.status_code == 200
    r = client.post('/creditors/3/activate', json={})
    assert r.status_code == 200
    r = client.post('/creditors/9223372036854775808/activate', json={})
    assert r.status_code == 200
    r = client.post('/creditors/18446744073709551615/activate', json={})
    assert r.status_code == 200

    r = client.get('/creditors-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CreditorsList'
    assert data['uri'] == '/creditors-list'
    assert data['itemsType'] == 'ObjectReference'
    assert data['first'] == '/creditors/9223372036854775808/enumerate'

    entries = _get_all_pages(client, data['first'], page_type='ObjectReferencesPage')
    assert entries == [
        {'uri': '/creditors/9223372036854775808/'},
        {'uri': '/creditors/18446744073709551615/'},
        {'uri': '/creditors/2/'},
        {'uri': '/creditors/3/'},
    ]


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
    assert log['type'] == 'PaginatedStream'
    assert log['first'] == '/creditors/2/log'
    assert log['forthcoming'] == '/creditors/2/log?prev=0'
    assert log['itemsType'] == 'LogEntry'
    dt = data['transfersList']
    assert dt['uri'] == '/creditors/2/transfers-list'
    ar = data['accountsList']
    assert ar['uri'] == '/creditors/2/accounts-list'


def test_get_log_page(client, creditor):
    r = client.get('/creditors/2222/log')
    assert r.status_code == 404

    r = client.get('/creditors/2/log')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'LogEntriesPage'
    assert data['items'] == []
    assert data['forthcoming'] == '?prev=0'
    assert 'next' not in data


def test_accounts_list_page(client, account):
    r = client.get('/creditors/2222/accounts-list')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountsList'
    assert data['uri'] == '/creditors/2/accounts-list'
    assert data['wallet'] == {'uri': '/creditors/2/wallet'}
    assert data['first'] == '/creditors/2/accounts/'
    assert data['itemsType'] == 'ObjectReference'
    assert data['latestUpdateId'] > 1
    assert iso8601.parse_date(data['latestUpdateAt'])

    r = client.get('/creditors/2/accounts/?prev=-1')
    assert r.status_code == 422

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
    p.process_pending_log_entries(2)
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 6
    assert [(e['objectType'], e['object']['uri']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/'),
        ('AccountsList', '/creditors/2/accounts-list'),
        ('Account', '/creditors/2/accounts/9223372036854775809/'),
        ('AccountsList', '/creditors/2/accounts-list'),
        ('Account', '/creditors/2/accounts/9223372036854775808/'),
        ('AccountsList', '/creditors/2/accounts-list'),
    ]
    assert all([e['deleted'] is False for e in entries])
    assert all(['data' not in e for e in entries])
    assert all([e['type'] == 'LogEntry' not in e for e in entries])
    assert all([iso8601.parse_date(e['addedAt']) not in e for e in entries])


def test_transfers_list_page(client, account, creditor):
    r = client.get('/creditors/2222/transfers-list')
    assert r.status_code == 404

    r = client.get('/creditors/2/transfers-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'TransfersList'
    assert data['uri'] == '/creditors/2/transfers-list'
    assert data['wallet'] == {'uri': '/creditors/2/wallet'}
    assert data['first'] == '/creditors/2/transfers/'
    assert data['itemsType'] == 'ObjectReference'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])

    r = client.get('/creditors/2/transfers/?prev=%#^')
    assert r.status_code == 422

    # no transfers
    assert _get_all_pages(client, '/creditors/2/transfers/', page_type='ObjectReferencesPage') == []

    request_data = {
        'type': 'TransferCreationRequest',
        'recipient': {'uri': 'swpt:1/2222'},
        'amount': 1000,
    }
    uuid_pattern = '123e4567-e89b-12d3-a456-426655440{}'

    # one transfer (one page)
    client.post('/creditors/2/transfers/', json={**request_data, "transferUuid": uuid_pattern.format('000')})
    items = _get_all_pages(client, '/creditors/2/transfers/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == ['123e4567-e89b-12d3-a456-426655440000']
    p.process_pending_log_entries(2)

    # three transfers (two pages)
    client.post('/creditors/2/transfers/', json={**request_data, "transferUuid": uuid_pattern.format('002')})
    p.process_pending_log_entries(2)
    client.post('/creditors/2/transfers/', json={**request_data, "transferUuid": uuid_pattern.format('001')})
    p.process_pending_log_entries(2)
    items = _get_all_pages(client, '/creditors/2/transfers/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == [
        '123e4567-e89b-12d3-a456-426655440000',
        '123e4567-e89b-12d3-a456-426655440001',
        '123e4567-e89b-12d3-a456-426655440002',
    ]

    # check log entires
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 8
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId')) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountsList', '/creditors/2/accounts-list', 2),
        ('Transfer', '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000', 1),
        ('TransfersList', '/creditors/2/transfers-list', 2),
        ('Transfer', '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440002', 1),
        ('TransfersList', '/creditors/2/transfers-list', 3),
        ('Transfer', '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440001', 1),
        ('TransfersList', '/creditors/2/transfers-list', 4),
    ]


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
    p.process_pending_log_entries(2)
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
        'accountsList': {'uri': '/creditors/2/accounts-list'},
        'createdAt': createdAt,
        'debtor': {
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
            'noteMaxBytes': 0,
            'safeToDelete': False,
            'latestUpdateAt': latestUpdateAt,
            'latestUpdateId': latestUpdateId,
        },
        'knowledge': {
            'type': 'AccountKnowledge',
            'uri': '/creditors/2/accounts/1/knowledge',
            'account': {'uri': '/creditors/2/accounts/1/'},
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
            'nextEntryId': 1,
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

    p.process_pending_log_entries(2)
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 2
    assert entries[1]['objectType'] == 'AccountsList'
    e = entries[0]
    assert e['type'] == 'LogEntry'
    assert e['entryId'] == 1
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
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    r = client.get('/creditors/2/accounts-list')
    assert r.status_code == 200
    data = r.get_json()
    latest_update_id = data['latestUpdateId']
    latest_update_at = iso8601.parse_date(data['latestUpdateAt'])

    p.process_pending_log_entries(2)
    r = client.delete('/creditors/2/accounts/1/')
    assert r.status_code == 204

    p.process_pending_log_entries(2)
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 11
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('deleted', False))
            for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1, False),
        ('AccountsList', '/creditors/2/accounts-list', 2, False),
        ('AccountConfig', '/creditors/2/accounts/1/config', 2, False),
        ('AccountsList', '/creditors/2/accounts-list', 3, False),
        ('Account', '/creditors/2/accounts/1/', None, True),
        ('AccountConfig', '/creditors/2/accounts/1/config', None, True),
        ('AccountInfo', '/creditors/2/accounts/1/info', None, True),
        ('AccountLedger', '/creditors/2/accounts/1/ledger', None, True),
        ('AccountDisplay', '/creditors/2/accounts/1/display', None, True),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', None, True),
        ('AccountKnowledge', '/creditors/2/accounts/1/knowledge', None, True),
    ]

    r = client.get('/creditors/2/accounts-list')
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
        'latestUpdateId': 2,
    }

    r = client.patch('/creditors/2/accounts/1111/config', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/config', json=request_data)
    assert r.status_code == 200
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

    request_data['negligibleAmount'] = 1.0
    r = client.patch('/creditors/2/accounts/1/config', json=request_data)
    assert r.status_code == 409

    p.process_pending_log_entries(2)
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 3
    assert [(e['objectType'], e['object']['uri']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/'),
        ('AccountsList', '/creditors/2/accounts-list'),
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
    assert data['amountDivisor'] == 1.0
    assert data['hide'] is False
    assert data['decimalPlaces'] == 0
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'unit' not in data
    assert 'debtorName' not in data
    p.process_pending_log_entries(2)

    request_data = {
        'type': 'AccountDisplay',
        'debtorName': 'United States of America',
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'unit': 'USD',
        'hide': True,
        'latestUpdateId': 2,
    }
    orig_request_data = request_data.copy()

    r = client.patch('/creditors/2/accounts/1111/display', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 200
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
    assert data['unit'] == 'USD'
    assert data['hide'] is True
    assert data['latestUpdateId'] == 2
    assert 'peg' not in data
    p.process_pending_log_entries(2)

    request_data['decimalPlaces'] = 1
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 409

    r = client.post('/creditors/2/accounts/', json={'uri': 'swpt:11'})
    assert r.status_code == 201
    p.process_pending_log_entries(2)

    r = client.patch('/creditors/2/accounts/11/display', json=orig_request_data)
    assert r.status_code == 422
    data = r.get_json()
    assert 'debtorName' in data['errors']['json']
    p.process_pending_log_entries(2)

    del request_data['debtorName']
    del request_data['unit']
    request_data['hide'] = True
    request_data['latestUpdateId'] = 3
    request_data['decimalPlaces'] = 3
    r = client.patch('/creditors/2/accounts/1/display', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['latestUpdateId'] == 3
    assert data['amountDivisor'] == 100.0
    assert data['hide'] is True
    assert data['decimalPlaces'] == 3
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'unit' not in data
    assert 'debtorName' not in data
    p.process_pending_log_entries(2)

    r = client.patch('/creditors/2/accounts/11/display', json=orig_request_data)
    assert r.status_code == 200
    p.process_pending_log_entries(2)

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 7
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountsList', '/creditors/2/accounts-list', 2),
        ('AccountDisplay', '/creditors/2/accounts/1/display', 2),
        ('Account', '/creditors/2/accounts/11/', 1),
        ('AccountsList', '/creditors/2/accounts-list', 3),
        ('AccountDisplay', '/creditors/2/accounts/1/display', 3),
        ('AccountDisplay', '/creditors/2/accounts/11/display', 2),
    ]
    assert all([entries[i]['entryId'] - entries[i - 1]['entryId'] == 1 for i in range(1, len(entries))])


def test_account_exchange(client, account):
    p.process_pending_log_entries(2)
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

    # Create another account, which is ready to be deleted.
    r = client.post('/creditors/2/accounts/', json={'uri': 'swpt:11'})
    assert r.status_code == 201
    p.process_pending_log_entries(2)
    r = client.patch('/creditors/2/accounts/11/config', json={
        'scheduledForDeletion': True,
        'negligibleAmount': m.DEFAULT_NEGLIGIBLE_AMOUNT,
        'allowUnsafeDeletion': True,
        'latestUpdateId': 2,
    })
    assert r.status_code == 200
    p.process_pending_log_entries(2)

    request_data = {
        'minPrincipal': 1000,
        'maxPrincipal': 2000,
        'latestUpdateId': 2,
    }

    r = client.patch('/creditors/2/accounts/1111/exchange', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    assert r.status_code == 200
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
    p.process_pending_log_entries(2)

    request_data['maxPrincipal'] = 3000
    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    assert r.status_code == 409
    data = r.get_json()
    assert 'latestUpdateId' in data['errors']['json']

    r = client.patch('/creditors/2/accounts/1/exchange', json={})
    assert r.status_code == 422
    data = r.get_json()
    assert 'latestUpdateId' in data['errors']['json']
    assert 'maxPrincipal' in data['errors']['json']
    assert 'minPrincipal' in data['errors']['json']

    request_data['policy'] = 'INVALID'
    request_data['latestUpdateId'] = 3
    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['policy'] == ['Invalid policy name.']

    request_data['policy'] = 'conservative'
    request_data['peg'] = {'exchangeRate': 1.5, 'account': {'uri': '/creditors/2/accounts/1111/'}}
    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['peg']['account']['uri'] == ['Account does not exist.']

    wrong_uris = [
        '/creditors/2/accounts/1111/',
        '/creditors/2/accounts/1111',
        '/creditors/2/accounts/',
        '/creditors/2/account-lookup',
        'awt4ao8t4o',
        'http://wrongname.com/creditors/2/accounts/11/',
        'https://example.com/creditors/2/accounts/11/',
        'http://example.com/creditors/2/accounts/11',
        '/creditors/2/accounts/11/?x=y',
        '/creditors/2/accounts/11/#xyz',
        'http://user:pass@example.com/creditors/2/accounts/11/',
        'http://[',
        '../1111/',
    ]
    for uri in wrong_uris:
        request_data['peg']['account']['uri'] = uri
        r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
        assert r.status_code == 422
        data = r.get_json()
        assert data['errors']['json']['peg']['account']['uri'] == ['Account does not exist.']

    request_data['peg']['account']['uri'] = '/creditors/2/accounts/11/'
    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['policy'] == 'conservative'
    assert data['latestUpdateId'] == 3
    p.process_pending_log_entries(2)

    ok_uris = [
        'http://example.com/creditors/2/accounts/11/',
        '../11/',
    ]
    for uri in ok_uris:
        request_data['peg']['account']['uri'] = uri
        r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
        assert r.status_code == 200

    r = client.delete('/creditors/2/accounts/11/')
    assert r.status_code == 403

    del request_data['peg']
    request_data['latestUpdateId'] = 4
    r = client.patch('/creditors/2/accounts/1/exchange', json=request_data)
    data = r.get_json()
    assert data['latestUpdateId'] == 4
    assert 'peg' not in data
    p.process_pending_log_entries(2)

    r = client.delete('/creditors/2/accounts/11/')
    assert r.status_code == 204
    p.process_pending_log_entries(2)

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) > 8
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries[:8]] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountsList', '/creditors/2/accounts-list', 2),
        ('Account', '/creditors/2/accounts/11/', 1),
        ('AccountsList', '/creditors/2/accounts-list', 3),
        ('AccountConfig', '/creditors/2/accounts/11/config', 2),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', 2),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', 3),
        ('AccountExchange', '/creditors/2/accounts/1/exchange', 4),
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
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'debtorInfo' not in data
    assert 'identity' not in data

    request_data = {
        'latestUpdateId': 2,
        'interestRate': 11.5,
        'interestRateChangedAt': '2020-01-01T00:00:00Z',
        'identity': {
            'type': 'AccountIdentity',
            'uri': 'swpt:1/2',
        },
        'debtorInfo': {
            'type': 'DebtorInfo',
            'iri': 'http://example.com',
            'contentType': 'text/html',
            'sha256': 64 * '0',
        },
        'nonStandardField': True,
    }

    r = client.patch('/creditors/2/accounts/1111/knowledge', json=[])
    assert r.status_code == 422

    r = client.patch('/creditors/2/accounts/1111/knowledge', json=1)
    assert r.status_code == 422

    r = client.patch('/creditors/2/accounts/1111/knowledge', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/2/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    r = client.patch('/creditors/2/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/2/accounts/1/knowledge'
    assert data['latestUpdateId'] == 2
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert data['interestRate'] == 11.5
    assert iso8601.parse_date(data['interestRateChangedAt']) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert data['identity'] == {'type': 'AccountIdentity', 'uri': 'swpt:1/2'}
    assert data['debtorInfo'] == {
        'type': 'DebtorInfo',
        'iri': 'http://example.com',
        'contentType': 'text/html',
        'sha256': 64 * '0',
    }
    assert data['nonStandardField'] is True

    request_data['addedField'] = 'value'
    r = client.patch('/creditors/2/accounts/1/knowledge', json=request_data)
    assert r.status_code == 409

    del request_data['debtorInfo']
    del request_data['identity']
    request_data['latestUpdateId'] = 3
    r = client.patch('/creditors/2/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/2/accounts/1/knowledge'
    assert data['interestRate'] == 11.5
    assert data['latestUpdateId'] == 3
    assert data['addedField'] == 'value'
    assert iso8601.parse_date(data['interestRateChangedAt']) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert 'debtorInfo' not in data
    assert 'identity' not in data

    p.process_pending_log_entries(2)
    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 4
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1),
        ('AccountsList', '/creditors/2/accounts-list', 2),
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
    assert data['account'] == {'uri': '/creditors/2/accounts/1/'}
    assert 'debtorInfo' not in data
    assert 'identity' not in data
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


def test_ledger_entries_list(ledger_entries, client, current_ts):
    r = client.get('/creditors/2222/accounts/1/entries?prev=100')
    assert r.status_code == 404 or r.get_json()['items'] == []

    r = client.get('/creditors/2/accounts/1111/entries?prev=100')
    assert r.status_code == 404 or r.get_json()['items'] == []

    items = _get_all_pages(client, '/creditors/2/accounts/1/entries?prev=100', page_type='LedgerEntriesPage')
    assert items == [
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/2/accounts/1/ledger'},
            'addedAt': current_ts.isoformat(),
            'entryId': 3,
            'aquiredAmount': 200,
            'principal': 350,
            'transfer': {'uri': '/creditors/2/accounts/1/transfers/0-2'},
        },
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/2/accounts/1/ledger'},
            'addedAt': current_ts.isoformat(),
            'entryId': 2,
            'aquiredAmount': 50,
            'principal': 150,
        },
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/2/accounts/1/ledger'},
            'addedAt': current_ts.isoformat(),
            'entryId': 1,
            'aquiredAmount': 100,
            'principal': 100,
            'transfer': {'uri': '/creditors/2/accounts/1/transfers/0-1'},
        },
    ]

    items = _get_all_pages(client, '/creditors/2/accounts/1/entries?prev=1', page_type='LedgerEntriesPage')
    assert len(items) == 0

    items = _get_all_pages(client, '/creditors/2/accounts/1/entries?prev=100&stop=1', page_type='LedgerEntriesPage')
    assert len(items) == 2

    items = _get_all_pages(client, '/creditors/2/accounts/1/entries?prev=3&stop=1', page_type='LedgerEntriesPage')
    assert len(items) == 1

    items = _get_all_pages(client, '/creditors/2/accounts/1/entries?prev=2&stop=1000', page_type='LedgerEntriesPage')
    assert len(items) == 0

    items = _get_all_pages(client, '/creditors/2/accounts/1/entries?prev=2&stop=2', page_type='LedgerEntriesPage')
    assert len(items) == 0

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 4
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('data'))
            for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1, None),
        ('AccountsList', '/creditors/2/accounts-list', 2, None),
        ('AccountLedger', '/creditors/2/accounts/1/ledger', 2, {'principal': 100, 'nextEntryId': 2}),
        ('AccountLedger', '/creditors/2/accounts/1/ledger', 3, {'principal': 350, 'nextEntryId': 4}),
    ]


def test_get_committed_transfer(client, account, current_ts):
    params = {
        'debtor_id': 1,
        'creditor_id': 2,
        'creation_date': date(1970, 1, 1),
        'transfer_number': 1,
        'coordinator_type': 'interest',
        'sender': '666',
        'recipient': '2',
        'acquired_amount': 100,
        'transfer_note_format': 'json',
        'transfer_note': '{"message": "test"}',
        'committed_at': current_ts,
        'principal': 1000,
        'ts': current_ts,
        'previous_transfer_number': 0,
        'retention_interval': timedelta(days=5),
    }
    p.process_account_transfer_signal(**params)

    r = client.get('/creditors/2/accounts/1/transfers/0-1')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CommittedTransfer'
    assert data['uri'] == '/creditors/2/accounts/1/transfers/0-1'
    assert data['committedAt'] == current_ts.isoformat()
    assert data['rationale'] == 'interest'
    assert data['noteFormat'] == 'json'
    assert data['note'] == '{"message": "test"}'
    assert data['account']['uri'] == '/creditors/2/accounts/1/'
    assert data['acquiredAmount'] == 100
    assert data['sender']['uri'] == 'swpt:1/666'
    assert data['recipient']['uri'] == 'swpt:1/2'

    r = client.get('/creditors/2/accounts/1/transfers/1-1')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/transfers/11111111111111111111111111111-1')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/transfers/INVALID')
    assert r.status_code == 404

    r = client.get('/creditors/2/accounts/1/transfers/1-0')
    assert r.status_code == 404


def test_create_transfer(client, account):
    p.process_pending_log_entries(2)

    request_data = {
        'type': 'TransferCreationRequest',
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
        'recipient': {'uri': 'swpt:1/2222'},
        'amount': 1000,
        'noteFormat': 'json',
        'note': '{"message": "test"}',
        'options': {
            'type': 'TransferOptions',
            'minInterestRate': -10,
            'deadline': '2009-08-24T14:15:22+00:00',
            'lockedAmount': 1000,
        },
    }

    r = client.post('/creditors/2222/transfers/', json=request_data)
    assert r.status_code == 404

    r = client.post('/creditors/2/transfers/', json=request_data)
    assert r.status_code == 201
    assert r.headers['location'] == 'http://example.com/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000'
    p.process_pending_log_entries(2)

    r = client.get('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Transfer'
    assert data['recipient']['uri'] == 'swpt:1/2222'
    assert data['amount'] == 1000
    assert data['note'] == '{"message": "test"}'
    assert data['noteFormat'] == 'json'
    assert iso8601.parse_date(data['initiatedAt'])
    assert data['transferUuid'] == '123e4567-e89b-12d3-a456-426655440000'
    assert data['latestUpdateId'] == 1
    assert iso8601.parse_date(data['latestUpdateAt'])
    assert 'result' not in data
    assert data['transfersList']['uri'] == '/creditors/2/transfers-list'
    assert iso8601.parse_date(data['checkupAt'])
    assert data['options'] == {
        'type': 'TransferOptions',
        'minInterestRate': -10.0,
        'deadline': '2009-08-24T14:15:22+00:00',
        'lockedAmount': 1000,
    }

    r = client.post('/creditors/2/transfers/', json=request_data)
    assert r.status_code == 303

    r = client.post('/creditors/2/transfers/', json={**request_data, 'amount': 999})
    assert r.status_code == 409

    r = client.post('/creditors/2/transfers/', json={**request_data, 'recipient': {'uri': 'INVALID'}})
    assert r.status_code == 422
    assert r.get_json()['errors']['json']['recipient']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440001', json={})
    assert r.status_code == 404

    r = client.post('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000', json={})
    assert r.status_code == 200
    r = client.post('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['result']['error']['errorCode'] == m.SC_CANCELED_BY_THE_SENDER
    assert data['latestUpdateId'] == 2
    p.process_pending_log_entries(2)

    r = client.delete('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440001')
    assert r.status_code == 204

    r = client.delete('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 204
    p.process_pending_log_entries(2)

    r = client.get('/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 404

    entries = _get_all_pages(client, '/creditors/2/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 7
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('deleted', False))
            for e in entries] == [
        ('Account', '/creditors/2/accounts/1/', 1, False),
        ('AccountsList', '/creditors/2/accounts-list', 2, False),
        ('Transfer', '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000', 1, False),
        ('TransfersList', '/creditors/2/transfers-list', 2, False),
        ('Transfer', '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000', 2, False),
        ('Transfer', '/creditors/2/transfers/123e4567-e89b-12d3-a456-426655440000', None, True),
        ('TransfersList', '/creditors/2/transfers-list', 3, False),
    ]


def test_unauthorized_creditor_id(creditor, client):
    r = client.get('/creditors/2/')
    assert r.status_code == 200

    r = client.get('/creditors/2/', headers={'X-Swpt-Creditor-Id': '*'})
    assert r.status_code == 200

    r = client.get('/creditors/2/', headers={'X-Swpt-Creditor-Id': '2'})
    assert r.status_code == 200

    r = client.get('/creditors/2/', headers={'X-Swpt-Creditor-Id': '1'})
    assert r.status_code == 401

    r = client.get('/creditors/18446744073709551615/', headers={'X-Swpt-Creditor-Id': '18446744073709551615'})
    assert r.status_code == 403

    with pytest.raises(ValueError):
        r = client.get('/creditors/18446744073709551615/', headers={'X-Swpt-Creditor-Id': '18446744073709551616'})

    with pytest.raises(ValueError):
        r = client.get('/creditors/18446744073709551615/', headers={'X-Swpt-Creditor-Id': '-1'})
