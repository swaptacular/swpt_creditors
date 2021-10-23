from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone, timedelta, date
import pytest
from swpt_lib.utils import u64_to_i64
from swpt_creditors import models as m
from swpt_creditors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def creditor(db_session):
    creditor = p.reserve_creditor(4294967296)
    p.activate_creditor(4294967296, creditor.reservation_id)
    return creditor


@pytest.fixture(scope='function')
def account(creditor):
    return p.create_new_account(4294967296, 1)


@pytest.fixture(scope='function')
def ledger_entries(db_session, account, current_ts):
    from swpt_creditors.procedures.account_updates import _update_ledger

    data = m.AccountData.query.one()
    db_session.add(_update_ledger(data, 1, 100, 100, current_ts))
    db_session.add(_update_ledger(data, 2, 200, 350, current_ts))
    db_session.commit()
    p.process_pending_log_entries(4294967296)


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
    r = client.post('/creditors/.creditor-reserve', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CreditorReservation'
    assert isinstance(data['creditorId'], str)
    assert isinstance(data['reservationId'], int)
    assert datetime.fromisoformat(data['validUntil'])
    assert datetime.fromisoformat(data['createdAt'])


def test_create_creditor(client):
    r = client.get('/creditors/4294967296/')
    assert r.status_code == 403

    r = client.post('/creditors/4294967296/reserve', headers={'X-Swpt-User-Id': 'creditors:2'}, json={})
    assert r.status_code == 403

    r = client.post('/creditors/4294967296/reserve', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CreditorReservation'
    assert data['creditorId'] == '4294967296'
    assert isinstance(data['reservationId'], int)
    assert datetime.fromisoformat(data['validUntil'])
    assert datetime.fromisoformat(data['createdAt'])
    reservation_id = data['reservationId']

    r = client.post('/creditors/4294967296/reserve', json={})
    assert r.status_code == 409

    r = client.get('/creditors/4294967296/')
    assert r.status_code == 403

    r = client.post('/creditors/4294967296/activate', json={
        'reservationId': 123,
    })
    assert r.status_code == 422
    assert 'reservationId' in r.get_json()['errors']['json']

    r = client.post('/creditors/4294967296/activate', json={
        'reservationId': reservation_id,
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/4294967296/'
    assert data['latestUpdateId'] == 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert datetime.fromisoformat(data['createdAt'])

    r = client.post('/creditors/4294967296/activate', json={
        'reservationId': reservation_id,
    })
    assert r.status_code == 200

    r = client.post('/creditors/4294967297/activate', json={
        'reservationId': 123,
    })
    assert r.status_code == 422
    assert 'reservationId' in r.get_json()['errors']['json']

    r = client.post('/creditors/4294967297/activate', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/4294967297/'
    assert data['latestUpdateId'] == 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert datetime.fromisoformat(data['createdAt'])

    r = client.post('/creditors/4294967297/activate', json={})
    assert r.status_code == 409

    r = client.get('/creditors/4294967296/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/4294967296/'
    assert data['latestUpdateId'] == 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert datetime.fromisoformat(data['createdAt'])

    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 0

    r = client.get('/creditors/4294967297/')
    assert r.status_code == 200

    r = client.post('/creditors/4294967297/deactivate', headers={'X-Swpt-User-Id': 'creditors:3'}, json={})
    assert r.status_code == 403

    r = client.post('/creditors/4294967297/deactivate', headers={'X-Swpt-User-Id': 'creditors-supervisor'}, json={})
    assert r.status_code == 403

    r = client.post('/creditors/4294967297/deactivate', headers={'X-Swpt-User-Id': 'creditors-superuser'}, json={})
    assert r.status_code == 204

    r = client.post('/creditors/4294967297/deactivate', json={})
    assert r.status_code == 204

    r = client.get('/creditors/4294967297/')
    assert r.status_code == 403

    r = client.post('/creditors/4294967297/deactivate', json={})
    assert r.status_code == 204


def test_get_creditors_list(client):
    r = client.post('/creditors/4294967296/reserve', json={})
    assert r.status_code == 200
    r = client.post('/creditors/4294967297/activate', json={})
    assert r.status_code == 200
    r = client.post('/creditors/4294967298/activate', json={})
    assert r.status_code == 200
    r = client.post('/creditors/4294967299/activate', json={})
    assert r.status_code == 200
    r = client.post('/creditors/8589934591/activate', json={})
    assert r.status_code == 200

    r = client.get('/creditors/.list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CreditorsList'
    assert data['uri'] == '/creditors/.list'
    assert data['itemsType'] == 'ObjectReference'
    assert data['first'] == '/creditors/9223372036854775808/enumerate'

    entries = _get_all_pages(client, data['first'], page_type='ObjectReferencesPage')
    assert entries == [
        {'uri': '/creditors/4294967297/'},
        {'uri': '/creditors/4294967298/'},
        {'uri': '/creditors/4294967299/'},
        {'uri': '/creditors/8589934591/'},
    ]


def test_change_pin(client, creditor):
    r = client.get('/creditors/4294967296/wallet', headers={'X-Swpt-Require-Pin': 'true'})
    assert r.status_code == 200
    data = r.get_json()
    assert data['requirePin'] is False
    assert data['pinInfo'] == {'uri': '/creditors/4294967296/pin'}

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'PinInfo'
    assert data['uri'] == '/creditors/4294967296/pin'
    assert data['status'] == 'off'
    assert data['latestUpdateId'] == 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['wallet'] == {'uri': '/creditors/4294967296/wallet'}

    r = client.patch('/creditors/4294967297/pin', json={
        'status': 'on',
        'newPin': '1234',
        'latestUpdateId': 2,
    })
    assert r.status_code == 404

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'on',
        'newPin': '1234',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'PinInfo'
    assert data['status'] == 'on'
    assert data['latestUpdateId'] == 2
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['wallet'] == {'uri': '/creditors/4294967296/wallet'}

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'on',
        'newPin': '1234',
        'latestUpdateId': 2,
    })
    assert r.status_code == 409

    r = client.patch('/creditors/4294967297/pin', headers={'X-Swpt-Require-Pin': 'true'}, json={
        'status': 'off',
        'latestUpdateId': 2,
    })
    assert r.status_code == 404

    r = client.patch('/creditors/4294967296/pin', headers={'X-Swpt-Require-Pin': 'true'}, json={
        'status': 'off',
        'latestUpdateId': 3,
    })
    assert r.status_code == 403

    r = client.patch('/creditors/4294967296/pin', headers={'X-Swpt-Require-Pin': 'true'}, json={
        'status': 'off',
        'pin': '1111',
        'latestUpdateId': 3,
    })
    assert r.status_code == 403

    for i in range(10):
        r = client.patch('/creditors/4294967296/pin', headers={'X-Swpt-Require-Pin': 'true'}, json={
            'status': 'off',
            'latestUpdateId': 3,
        })
        assert r.status_code == 403

    r = client.get('/creditors/4294967296/wallet', headers={'X-Swpt-Require-Pin': 'true'})
    assert r.status_code == 200
    data = r.get_json()
    assert data['requirePin'] is True
    assert data['pinInfo'] == {'uri': '/creditors/4294967296/pin'}

    r = client.get('/creditors/4294967296/wallet', headers={'X-Swpt-Require-Pin': 'false'})
    assert r.status_code == 200
    data = r.get_json()
    assert data['requirePin'] is False

    r = client.get('/creditors/4294967296/wallet')
    assert r.status_code == 200
    data = r.get_json()
    assert data['requirePin'] is False

    r = client.patch('/creditors/4294967296/pin', headers={'X-Swpt-Require-Pin': 'true'}, json={
        'status': 'off',
        'pin': '1234',
        'latestUpdateId': 3,
    })
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'off'
    assert data['latestUpdateId'] == 3

    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 2
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('deleted', False))
            for e in entries] == [
        ('PinInfo', '/creditors/4294967296/pin', 2, False),
        ('PinInfo', '/creditors/4294967296/pin', 3, False),
    ]


def test_redirect_to_wallet(client, creditor):
    r = client.get('/creditors/.wallet')
    assert r.status_code == 204

    r = client.get('/creditors/.wallet', headers={'X-Swpt-User-Id': 'creditors:4294967296'})
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://localhost/creditors/4294967296/wallet'

    r = client.get('/creditors/.wallet', headers={'X-Swpt-User-Id': 'creditors:18446744073709551615'})
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://localhost/creditors/18446744073709551615/wallet'


def test_get_wallet(client, creditor):
    r = client.get('/creditors/4294967299/wallet')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/wallet')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Wallet'
    assert data['uri'] == '/creditors/4294967296/wallet'
    assert data['pinInfo'] == {'uri': '/creditors/4294967296/pin'}
    assert data['requirePin'] is False
    assert data['creditor'] == {'uri': '/creditors/4294967296/'}
    assert data['logRetentionDays'] == 31
    assert data['logLatestEntryId'] == 0
    log = data['log']
    assert log['type'] == 'PaginatedStream'
    assert log['first'] == '/creditors/4294967296/log'
    assert log['forthcoming'] == '/creditors/4294967296/log?prev=0'
    assert log['itemsType'] == 'LogEntry'
    dt = data['transfersList']
    assert dt['uri'] == '/creditors/4294967296/transfers-list'
    ar = data['accountsList']
    assert ar['uri'] == '/creditors/4294967296/accounts-list'


def test_get_log_page(client, creditor):
    r = client.get('/creditors/4294967299/log')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/log')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'LogEntriesPage'
    assert data['items'] == []
    assert data['forthcoming'] == '?prev=0'
    assert 'next' not in data


def test_accounts_list_page(client, account):
    r = client.get('/creditors/4294967299/accounts-list')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountsList'
    assert data['uri'] == '/creditors/4294967296/accounts-list'
    assert data['wallet'] == {'uri': '/creditors/4294967296/wallet'}
    assert data['first'] == '/creditors/4294967296/accounts/'
    assert data['itemsType'] == 'ObjectReference'
    assert data['latestUpdateId'] > 1
    assert datetime.fromisoformat(data['latestUpdateAt'])

    r = client.get('/creditors/4294967296/accounts/?prev=-1')
    assert r.status_code == 422

    # one account (one page)
    items = _get_all_pages(client, '/creditors/4294967296/accounts/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == ['1/']

    # add two more accounts
    r = client.post('/creditors/4294967296/accounts/',
                    json={'type': 'DebtorIdentity', 'uri': 'swpt:9223372036854775809'})
    assert r.status_code == 201
    r = client.post('/creditors/4294967296/accounts/',
                    json={'type': 'DebtorIdentity', 'uri': 'swpt:9223372036854775808'})
    assert r.status_code == 201

    # three accounts (two pages)
    items = _get_all_pages(client, '/creditors/4294967296/accounts/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == ['9223372036854775808/', '9223372036854775809/', '1/']
    assert u64_to_i64(9223372036854775808) < u64_to_i64(9223372036854775809) < u64_to_i64(1)

    # check log entires
    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 6
    assert [(e['objectType'], e['object']['uri']) for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/'),
        ('AccountsList', '/creditors/4294967296/accounts-list'),
        ('Account', '/creditors/4294967296/accounts/9223372036854775809/'),
        ('AccountsList', '/creditors/4294967296/accounts-list'),
        ('Account', '/creditors/4294967296/accounts/9223372036854775808/'),
        ('AccountsList', '/creditors/4294967296/accounts-list'),
    ]
    assert all([e['deleted'] is False for e in entries])
    assert all(['data' not in e for e in entries])
    assert all([e['type'] == 'LogEntry' not in e for e in entries])
    assert all([datetime.fromisoformat(e['addedAt']) not in e for e in entries])


def test_transfers_list_page(client, account, creditor):
    r = client.get('/creditors/4294967299/transfers-list')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/transfers-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'TransfersList'
    assert data['uri'] == '/creditors/4294967296/transfers-list'
    assert data['wallet'] == {'uri': '/creditors/4294967296/wallet'}
    assert data['first'] == '/creditors/4294967296/transfers/'
    assert data['itemsType'] == 'ObjectReference'
    assert data['latestUpdateId'] == 1
    assert datetime.fromisoformat(data['latestUpdateAt'])

    r = client.get('/creditors/4294967296/transfers/?prev=%#^')
    assert r.status_code == 422

    # no transfers
    assert _get_all_pages(client, '/creditors/4294967296/transfers/', page_type='ObjectReferencesPage') == []

    request_data = {
        'type': 'TransferCreationRequest',
        'recipient': {'uri': 'swpt:1/4294967299'},
        'amount': 1000,
    }
    uuid_pattern = '123e4567-e89b-12d3-a456-426655440{}'

    # one transfer (one page)
    client.post('/creditors/4294967296/transfers/', json={**request_data, "transferUuid": uuid_pattern.format('000')})
    items = _get_all_pages(client, '/creditors/4294967296/transfers/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == ['123e4567-e89b-12d3-a456-426655440000']
    p.process_pending_log_entries(4294967296)

    # three transfers (two pages)
    client.post('/creditors/4294967296/transfers/', json={**request_data, "transferUuid": uuid_pattern.format('002')})
    p.process_pending_log_entries(4294967296)
    client.post('/creditors/4294967296/transfers/', json={**request_data, "transferUuid": uuid_pattern.format('001')})
    p.process_pending_log_entries(4294967296)
    items = _get_all_pages(client, '/creditors/4294967296/transfers/', page_type='ObjectReferencesPage')
    assert [item['uri'] for item in items] == [
        '123e4567-e89b-12d3-a456-426655440000',
        '123e4567-e89b-12d3-a456-426655440001',
        '123e4567-e89b-12d3-a456-426655440002',
    ]

    # check log entires
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    account_uid = p.get_account(4294967296, 1).latest_update_id
    assert len(entries) == 8
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId')) for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2),
        ('Transfer', '/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000', 1),
        ('TransfersList', '/creditors/4294967296/transfers-list', 2),
        ('Transfer', '/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440002', 1),
        ('TransfersList', '/creditors/4294967296/transfers-list', 3),
        ('Transfer', '/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440001', 1),
        ('TransfersList', '/creditors/4294967296/transfers-list', 4),
    ]


def test_account_lookup(client, creditor):
    r = client.post('/creditors/4294967296/account-lookup',
                    json={'type': 'AccountIdentity', 'uri': 'xxx:1/4294967296'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/4294967296/account-lookup',
                    json={'type': 'AccountIdentity', 'uri': 'swpt:1/4294967296'})
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'DebtorIdentity'
    assert data['uri'] == 'swpt:1'


def test_debtor_lookup(client, account):
    r = client.post('/creditors/4294967296/debtor-lookup',
                    json={'type': 'DebtorIdentity', 'uri': 'xxx:4294967296'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/4294967296/debtor-lookup', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://localhost/creditors/4294967296/accounts/1/'

    r = client.post('/creditors/4294967296/debtor-lookup', json={'type': 'DebtorIdentity', 'uri': 'swpt:1111'})
    assert r.status_code == 204
    assert r.data == b''


def test_create_account(client, creditor):
    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 0

    r = client.post('/creditors/4294967296/accounts/', json={'type': 'DebtorIdentity', 'uri': 'xxx:1'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/4294967296/accounts/', json={
        'type': 'DebtorIdentity', 'uri': 'swpt:1', 'unknowFiled': 'test'})
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json'] == {'unknowFiled': ['Unknown field.']}

    r = client.post('/creditors/4294967296/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 201
    data1 = r.get_json()
    assert r.headers['Location'] == 'http://localhost/creditors/4294967296/accounts/1/'
    latestUpdateId = data1['latestUpdateId']
    latestUpdateAt = data1['latestUpdateAt']
    createdAt = data1['createdAt']
    assert latestUpdateId >= 1
    assert datetime.fromisoformat(latestUpdateAt)
    assert datetime.fromisoformat(createdAt)
    assert data1['config']['latestUpdateId'] >= 1
    assert data1['display']['latestUpdateId'] >= 1
    assert data1['exchange']['latestUpdateId'] >= 1
    assert data1['info']['latestUpdateId'] >= 1
    assert data1['ledger']['latestUpdateId'] >= 1
    assert data1['ledger']['nextEntryId'] >= 1
    assert data1['knowledge']['latestUpdateId'] >= 1
    nextEntryId = data1['ledger']['nextEntryId']
    del data1['config']['latestUpdateId']
    del data1['display']['latestUpdateId']
    del data1['exchange']['latestUpdateId']
    del data1['info']['latestUpdateId']
    del data1['ledger']['latestUpdateId']
    del data1['knowledge']['latestUpdateId']
    assert data1 == {
        'type': 'Account',
        'uri': '/creditors/4294967296/accounts/1/',
        'accountsList': {'uri': '/creditors/4294967296/accounts-list'},
        'createdAt': createdAt,
        'debtor': {
            'type': 'DebtorIdentity',
            'uri': 'swpt:1',
        },
        'config': {
            'type': 'AccountConfig',
            'uri': '/creditors/4294967296/accounts/1/config',
            'account': {'uri': '/creditors/4294967296/accounts/1/'},
            'allowUnsafeDeletion': False,
            'negligibleAmount': 1e+30,
            'scheduledForDeletion': False,
            'latestUpdateAt': latestUpdateAt,
        },
        'display': {
            'type': 'AccountDisplay',
            'uri': '/creditors/4294967296/accounts/1/display',
            'account': {'uri': '/creditors/4294967296/accounts/1/'},
            'amountDivisor': 1.0,
            'decimalPlaces': 0,
            'hide': False,
            'latestUpdateAt': latestUpdateAt,
        },
        'exchange': {
            'type': 'AccountExchange',
            'uri': '/creditors/4294967296/accounts/1/exchange',
            'account': {'uri': '/creditors/4294967296/accounts/1/'},
            'minPrincipal': -9223372036854775808,
            'maxPrincipal': 9223372036854775807,
            'latestUpdateAt': latestUpdateAt,
        },
        'info': {
            'type': 'AccountInfo',
            'uri': '/creditors/4294967296/accounts/1/info',
            'account': {'uri': '/creditors/4294967296/accounts/1/'},
            'interestRate': 0.0,
            'interestRateChangedAt': '1970-01-01T00:00:00+00:00',
            'noteMaxBytes': 0,
            'safeToDelete': False,
            'latestUpdateAt': latestUpdateAt,
        },
        'knowledge': {
            'type': 'AccountKnowledge',
            'uri': '/creditors/4294967296/accounts/1/knowledge',
            'account': {'uri': '/creditors/4294967296/accounts/1/'},
            'latestUpdateAt': latestUpdateAt,
        },
        'ledger': {
            'type': 'AccountLedger',
            'uri': '/creditors/4294967296/accounts/1/ledger',
            'account': {'uri': '/creditors/4294967296/accounts/1/'},
            'principal': 0,
            'interest': 0,
            'entries': {
                'first': f'/creditors/4294967296/accounts/1/entries?prev={nextEntryId}',
                'itemsType': 'LedgerEntry',
                'type': 'PaginatedList',
            },
            'nextEntryId': nextEntryId,
            'latestUpdateAt': latestUpdateAt,
        },
        'latestUpdateAt': latestUpdateAt,
        'latestUpdateId': latestUpdateId,
    }

    r = client.post('/creditors/4294967296/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 303
    assert r.headers['Location'] == 'http://localhost/creditors/4294967296/accounts/1/'

    r = client.post('/creditors/4294967299/accounts/', json={'type': 'DebtorIdentity', 'uri': 'swpt:1'})
    assert r.status_code == 404

    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 2
    assert entries[1]['objectType'] == 'AccountsList'
    e = entries[0]
    assert e['type'] == 'LogEntry'
    assert e['entryId'] == 1
    assert e['objectType'] == 'Account'
    assert e['object'] == {'uri': '/creditors/4294967296/accounts/1/'}
    assert not e.get('deleted')
    assert datetime.fromisoformat(e['addedAt'])

    r = client.get('/creditors/4294967296/accounts/1/')
    assert r.status_code == 200
    data2 = r.get_json()
    del data2['config']['latestUpdateId']
    del data2['display']['latestUpdateId']
    del data2['exchange']['latestUpdateId']
    del data2['info']['latestUpdateId']
    del data2['ledger']['latestUpdateId']
    del data2['knowledge']['latestUpdateId']
    assert data1 == data2


def test_get_account(client, account):
    r = client.get('/creditors/4294967296/accounts/1111/')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Account'
    assert data['uri'] == '/creditors/4294967296/accounts/1/'


def test_delete_account(client, account):
    r = client.delete('/creditors/4294967296/accounts/1111/', headers={'X-Swpt-User-Id': 'creditors-supervisor'})
    assert r.status_code == 403

    r = client.delete('/creditors/4294967296/accounts/1111/', headers={'X-Swpt-User-Id': 'creditors-superuser'})
    assert r.status_code == 204

    r = client.delete('/creditors/4294967296/accounts/1111/', headers={'X-Swpt-User-Id': 'creditors:4294967296'})
    assert r.status_code == 204

    r = client.delete('/creditors/4294967296/accounts/1111/')
    assert r.status_code == 204

    r = client.delete('/creditors/4294967299/accounts/1/')
    assert r.status_code == 204

    r = client.delete('/creditors/4294967296/accounts/1/')
    assert r.status_code == 403

    latestUpdateId = p.get_account_config(4294967296, 1).config_latest_update_id
    r = client.patch('/creditors/4294967296/accounts/1/config', json={
        'scheduledForDeletion': True,
        'negligibleAmount': m.DEFAULT_NEGLIGIBLE_AMOUNT,
        'allowUnsafeDeletion': True,
        'latestUpdateId': latestUpdateId + 1,
    })
    assert r.status_code == 200

    r = client.get('/creditors/4294967296/accounts-list')
    assert r.status_code == 200
    data = r.get_json()
    latest_update_id = data['latestUpdateId']
    latest_update_at = datetime.fromisoformat(data['latestUpdateAt'])

    account_uid = p.get_account(4294967296, 1).latest_update_id
    p.process_pending_log_entries(4294967296)
    r = client.delete('/creditors/4294967296/accounts/1/')
    assert r.status_code == 204

    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 11
    object_update_id = entries[4]['objectUpdateId']
    assert object_update_id > account_uid
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('deleted', False))
            for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid, False),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2, False),
        ('AccountConfig', '/creditors/4294967296/accounts/1/config', latestUpdateId + 1, False),
        ('AccountsList', '/creditors/4294967296/accounts-list', 3, False),
        ('Account', '/creditors/4294967296/accounts/1/', object_update_id, True),
        ('AccountConfig', '/creditors/4294967296/accounts/1/config', object_update_id, True),
        ('AccountInfo', '/creditors/4294967296/accounts/1/info', object_update_id, True),
        ('AccountLedger', '/creditors/4294967296/accounts/1/ledger', object_update_id, True),
        ('AccountDisplay', '/creditors/4294967296/accounts/1/display', object_update_id, True),
        ('AccountExchange', '/creditors/4294967296/accounts/1/exchange', object_update_id, True),
        ('AccountKnowledge', '/creditors/4294967296/accounts/1/knowledge', object_update_id, True),
    ]

    r = client.get('/creditors/4294967296/accounts-list')
    assert r.status_code == 200
    data = r.get_json()
    assert data['latestUpdateId'] == latest_update_id + 1
    assert datetime.fromisoformat(data['latestUpdateAt']) >= latest_update_at


def test_account_config(client, account):
    r = client.get('/creditors/4294967296/accounts/1111/config')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/config')
    assert r.status_code == 200
    data = r.get_json()
    latestUpdateId = data['latestUpdateId']
    assert data['type'] == 'AccountConfig'
    assert data['uri'] == '/creditors/4294967296/accounts/1/config'
    assert latestUpdateId >= 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['scheduledForDeletion'] is False
    assert data['allowUnsafeDeletion'] is False
    assert data['negligibleAmount'] == m.DEFAULT_NEGLIGIBLE_AMOUNT
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}

    request_data = {
        'pin': '1234',
        'negligibleAmount': 100.0,
        'allowUnsafeDeletion': True,
        'scheduledForDeletion': True,
        'latestUpdateId': latestUpdateId + 1,
    }

    r = client.patch('/creditors/4294967296/accounts/1111/config', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/4294967296/accounts/1/config', json=request_data)
    assert r.status_code == 200
    r = client.patch('/creditors/4294967296/accounts/1/config', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountConfig'
    assert data['uri'] == '/creditors/4294967296/accounts/1/config'
    assert data['latestUpdateId'] == latestUpdateId + 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['scheduledForDeletion'] is True
    assert data['allowUnsafeDeletion'] is True
    assert data['negligibleAmount'] == 100.0
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}

    request_data['negligibleAmount'] = 1.0
    r = client.patch('/creditors/4294967296/accounts/1/config', json=request_data)
    assert r.status_code == 409

    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 3
    assert [(e['objectType'], e['object']['uri']) for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/'),
        ('AccountsList', '/creditors/4294967296/accounts-list'),
        ('AccountConfig', '/creditors/4294967296/accounts/1/config'),
    ]

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'blocked',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    r = client.patch('/creditors/4294967296/accounts/1/config',
                     headers={'X-Swpt-Require-Pin': 'true'},
                     json=request_data)
    assert r.status_code == 403


def test_account_display(client, account):
    r = client.get('/creditors/4294967296/accounts/1111/display')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/display')
    assert r.status_code == 200
    data = r.get_json()
    latestUpdateId = data['latestUpdateId']
    assert data['type'] == 'AccountDisplay'
    assert data['uri'] == '/creditors/4294967296/accounts/1/display'
    assert data['latestUpdateId'] >= 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['amountDivisor'] == 1.0
    assert data['hide'] is False
    assert data['decimalPlaces'] == 0
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}
    assert 'unit' not in data
    assert 'debtorName' not in data
    p.process_pending_log_entries(4294967296)

    request_data = {
        'type': 'AccountDisplay',
        'debtorName': 'United States of America',
        'amountDivisor': 100.0,
        'decimalPlaces': 2,
        'unit': 'USD',
        'hide': True,
        'latestUpdateId': latestUpdateId + 1,
        'pin': '1234',
    }
    orig_request_data = request_data.copy()

    r = client.patch('/creditors/4294967296/accounts/1111/display', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/4294967296/accounts/1/display', json=request_data)
    assert r.status_code == 200
    r = client.patch('/creditors/4294967296/accounts/1/display', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountDisplay'
    assert data['uri'] == '/creditors/4294967296/accounts/1/display'
    assert data['latestUpdateId'] == latestUpdateId + 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['debtorName'] == 'United States of America'
    assert data['amountDivisor'] == 100.0
    assert data['decimalPlaces'] == 2
    assert data['unit'] == 'USD'
    assert data['hide'] is True
    assert 'peg' not in data
    p.process_pending_log_entries(4294967296)

    request_data['decimalPlaces'] = 1
    r = client.patch('/creditors/4294967296/accounts/1/display', json=request_data)
    assert r.status_code == 409

    r = client.post('/creditors/4294967296/accounts/', json={'uri': 'swpt:11'})
    assert r.status_code == 201
    p.process_pending_log_entries(4294967296)

    latestUpdateId_11 = p.get_account_display(4294967296, 11).latest_update_id
    r = client.patch('/creditors/4294967296/accounts/11/display', json={
        **orig_request_data, 'latestUpdateId': latestUpdateId_11 + 1})
    assert r.status_code == 422
    data = r.get_json()
    assert 'debtorName' in data['errors']['json']
    p.process_pending_log_entries(4294967296)

    del request_data['debtorName']
    del request_data['unit']
    request_data['hide'] = True
    request_data['latestUpdateId'] = latestUpdateId + 2
    request_data['decimalPlaces'] = 3
    r = client.patch('/creditors/4294967296/accounts/1/display', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['latestUpdateId'] == latestUpdateId + 2
    assert data['amountDivisor'] == 100.0
    assert data['hide'] is True
    assert data['decimalPlaces'] == 3
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}
    assert 'unit' not in data
    assert 'debtorName' not in data
    p.process_pending_log_entries(4294967296)

    r = client.patch('/creditors/4294967296/accounts/11/display', json={
        **orig_request_data, 'latestUpdateId': latestUpdateId_11 + 1})
    assert r.status_code == 200
    p.process_pending_log_entries(4294967296)

    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    account_uid_1 = p.get_account(4294967296, 1).latest_update_id
    account_uid_11 = p.get_account(4294967296, 11).latest_update_id
    assert len(entries) == 7
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid_1),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2),
        ('AccountDisplay', '/creditors/4294967296/accounts/1/display', latestUpdateId + 1),
        ('Account', '/creditors/4294967296/accounts/11/', account_uid_11),
        ('AccountsList', '/creditors/4294967296/accounts-list', 3),
        ('AccountDisplay', '/creditors/4294967296/accounts/1/display', latestUpdateId + 2),
        ('AccountDisplay', '/creditors/4294967296/accounts/11/display', latestUpdateId_11 + 1),
    ]
    assert all([entries[i]['entryId'] - entries[i - 1]['entryId'] == 1 for i in range(1, len(entries))])

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'blocked',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    r = client.patch('/creditors/4294967296/accounts/1/display',
                     headers={'X-Swpt-Require-Pin': 'true'},
                     json=request_data)
    assert r.status_code == 403


def test_account_exchange(client, account):
    p.process_pending_log_entries(4294967296)
    r = client.get('/creditors/4294967296/accounts/1111/exchange')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/exchange')
    assert r.status_code == 200
    data = r.get_json()
    latestUpdateId = data['latestUpdateId']
    assert data['type'] == 'AccountExchange'
    assert data['uri'] == '/creditors/4294967296/accounts/1/exchange'
    assert data['latestUpdateId'] >= 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['minPrincipal'] == p.MIN_INT64
    assert data['maxPrincipal'] == p.MAX_INT64
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}
    assert 'policy' not in data

    # Create another account, which is ready to be deleted.
    r = client.post('/creditors/4294967296/accounts/', json={'uri': 'swpt:11'})
    assert r.status_code == 201
    config_latest_update_id = p.get_account_config(4294967296, 11).config_latest_update_id
    p.process_pending_log_entries(4294967296)
    r = client.patch('/creditors/4294967296/accounts/11/config', json={
        'scheduledForDeletion': True,
        'negligibleAmount': m.DEFAULT_NEGLIGIBLE_AMOUNT,
        'allowUnsafeDeletion': True,
        'latestUpdateId': config_latest_update_id + 1,
    })
    assert r.status_code == 200
    p.process_pending_log_entries(4294967296)

    request_data = {
        'minPrincipal': 1000,
        'maxPrincipal': 2000,
        'latestUpdateId': latestUpdateId + 1,
    }

    r = client.patch('/creditors/4294967296/accounts/1111/exchange', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    assert r.status_code == 200
    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountExchange'
    assert data['uri'] == '/creditors/4294967296/accounts/1/exchange'
    assert data['latestUpdateId'] == latestUpdateId + 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['minPrincipal'] == 1000
    assert data['maxPrincipal'] == 2000
    assert 'policy' not in data
    p.process_pending_log_entries(4294967296)

    request_data['maxPrincipal'] = 3000
    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    assert r.status_code == 409
    data = r.get_json()
    assert 'latestUpdateId' in data['errors']['json']

    r = client.patch('/creditors/4294967296/accounts/1/exchange', json={})
    assert r.status_code == 422
    data = r.get_json()
    assert 'latestUpdateId' in data['errors']['json']
    assert 'maxPrincipal' in data['errors']['json']
    assert 'minPrincipal' in data['errors']['json']

    request_data['policy'] = 'INVALID'
    request_data['latestUpdateId'] = latestUpdateId + 2
    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['policy'] == ['Invalid policy name.']

    request_data['policy'] = 'conservative'
    request_data['peg'] = {'exchangeRate': 1.5, 'account': {'uri': '/creditors/4294967296/accounts/1111/'}}
    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    assert r.status_code == 422
    data = r.get_json()
    assert data['errors']['json']['peg']['account']['uri'] == ['Account does not exist.']

    wrong_uris = [
        '/creditors/4294967296/accounts/1111/',
        '/creditors/4294967296/accounts/1111',
        '/creditors/4294967296/accounts/',
        '/creditors/4294967296/account-lookup',
        'awt4ao8t4o',
        'http://wrongname.com/creditors/4294967296/accounts/11/',
        'https://localhost/creditors/4294967296/accounts/11/',
        'http://localhost/creditors/4294967296/accounts/11',
        '/creditors/4294967296/accounts/11/?x=y',
        '/creditors/4294967296/accounts/11/#xyz',
        'http://user:pass@localhost/creditors/4294967296/accounts/11/',
        'http://[',
        '../1111/',
    ]
    for uri in wrong_uris:
        request_data['peg']['account']['uri'] = uri
        r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
        assert r.status_code == 422
        data = r.get_json()
        assert data['errors']['json']['peg']['account']['uri'] == ['Account does not exist.']

    request_data['peg']['account']['uri'] = '/creditors/4294967296/accounts/11/'
    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['policy'] == 'conservative'
    assert data['latestUpdateId'] == latestUpdateId + 2
    p.process_pending_log_entries(4294967296)

    ok_uris = [
        'http://localhost/creditors/4294967296/accounts/11/',
        '../11/',
    ]
    for uri in ok_uris:
        request_data['peg']['account']['uri'] = uri
        r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
        assert r.status_code == 200

    r = client.delete('/creditors/4294967296/accounts/11/')
    assert r.status_code == 403

    del request_data['peg']
    request_data['latestUpdateId'] = latestUpdateId + 3
    r = client.patch('/creditors/4294967296/accounts/1/exchange', json=request_data)
    data = r.get_json()
    assert data['latestUpdateId'] == latestUpdateId + 3
    assert 'peg' not in data
    p.process_pending_log_entries(4294967296)

    account_uid_11 = p.get_account(4294967296, 11).latest_update_id
    r = client.delete('/creditors/4294967296/accounts/11/')
    assert r.status_code == 204
    p.process_pending_log_entries(4294967296)

    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    account_uid_1 = p.get_account(4294967296, 1).latest_update_id
    assert len(entries) > 8
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries[:8]] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid_1),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2),
        ('Account', '/creditors/4294967296/accounts/11/', account_uid_11),
        ('AccountsList', '/creditors/4294967296/accounts-list', 3),
        ('AccountConfig', '/creditors/4294967296/accounts/11/config', config_latest_update_id + 1),
        ('AccountExchange', '/creditors/4294967296/accounts/1/exchange', latestUpdateId + 1),
        ('AccountExchange', '/creditors/4294967296/accounts/1/exchange', latestUpdateId + 2),
        ('AccountExchange', '/creditors/4294967296/accounts/1/exchange', latestUpdateId + 3),
    ]

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'blocked',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    r = client.patch('/creditors/4294967296/accounts/1/exchange',
                     headers={'X-Swpt-Require-Pin': 'true'},
                     json=request_data)
    assert r.status_code == 403


def test_account_knowledge(client, account):
    r = client.get('/creditors/4294967296/accounts/1111/knowledge')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/knowledge')
    assert r.status_code == 200
    data = r.get_json()
    latestUpdateId = data['latestUpdateId']
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/4294967296/accounts/1/knowledge'
    assert data['latestUpdateId'] >= 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}
    assert 'debtorInfo' not in data
    assert 'identity' not in data

    request_data = {
        'latestUpdateId': latestUpdateId + 1,
        'interestRate': 11.5,
        'interestRateChangedAt': '2020-01-01T00:00:00+00:00',
        'identity': {
            'type': 'AccountIdentity',
            'uri': 'swpt:1/4294967296',
        },
        'debtorInfo': {
            'type': 'DebtorInfo',
            'iri': 'http://example.com',
            'contentType': 'text/html',
            'sha256': 64 * '0',
        },
        'nonStandardField': True,
    }

    r = client.patch('/creditors/4294967296/accounts/1111/knowledge', json=[])
    assert r.status_code == 422

    r = client.patch('/creditors/4294967296/accounts/1111/knowledge', json=1)
    assert r.status_code == 422

    r = client.patch('/creditors/4294967296/accounts/1111/knowledge', json=request_data)
    assert r.status_code == 404

    r = client.patch('/creditors/4294967296/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    r = client.patch('/creditors/4294967296/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/4294967296/accounts/1/knowledge'
    assert data['latestUpdateId'] == latestUpdateId + 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['interestRate'] == 11.5
    assert datetime.fromisoformat(data['interestRateChangedAt']) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert data['identity'] == {'type': 'AccountIdentity', 'uri': 'swpt:1/4294967296'}
    assert data['debtorInfo'] == {
        'type': 'DebtorInfo',
        'iri': 'http://example.com',
        'contentType': 'text/html',
        'sha256': 64 * '0',
    }
    assert data['nonStandardField'] is True

    request_data['addedField'] = 'value'
    r = client.patch('/creditors/4294967296/accounts/1/knowledge', json=request_data)
    assert r.status_code == 409

    del request_data['debtorInfo']
    del request_data['identity']
    request_data['latestUpdateId'] = latestUpdateId + 2
    r = client.patch('/creditors/4294967296/accounts/1/knowledge', json=request_data)
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountKnowledge'
    assert data['uri'] == '/creditors/4294967296/accounts/1/knowledge'
    assert data['interestRate'] == 11.5
    assert data['latestUpdateId'] == latestUpdateId + 2
    assert data['addedField'] == 'value'
    assert datetime.fromisoformat(data['interestRateChangedAt']) == datetime(2020, 1, 1, tzinfo=timezone.utc)
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert 'debtorInfo' not in data
    assert 'identity' not in data

    p.process_pending_log_entries(4294967296)
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    account_uid = p.get_account(4294967296, 1).latest_update_id
    assert len(entries) == 4
    assert [(e['objectType'], e['object']['uri'], e['objectUpdateId']) for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2),
        ('AccountKnowledge', '/creditors/4294967296/accounts/1/knowledge', latestUpdateId + 1),
        ('AccountKnowledge', '/creditors/4294967296/accounts/1/knowledge', latestUpdateId + 2),
    ]


def test_get_account_info(client, account):
    r = client.get('/creditors/4294967296/accounts/1111/info')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/info')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountInfo'
    assert data['uri'] == '/creditors/4294967296/accounts/1/info'
    assert data['latestUpdateId'] >= 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert datetime.fromisoformat(data['interestRateChangedAt']) == m.TS0
    assert data['interestRate'] == 0.0
    assert data['safeToDelete'] is False
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}
    assert 'debtorInfo' not in data
    assert 'identity' not in data
    assert 'configError' not in data


def test_get_account_ledger(client, account):
    r = client.get('/creditors/4294967296/accounts/1111/ledger')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/ledger')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'AccountLedger'
    assert data['uri'] == '/creditors/4294967296/accounts/1/ledger'
    assert data['latestUpdateId'] >= 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert data['principal'] == 0
    assert data['interest'] == 0
    assert 'latestEntryId' not in data
    assert data['nextEntryId'] >= 1
    next_entry_id = data['nextEntryId']
    assert data['entries'] == {
        'itemsType': 'LedgerEntry',
        'type': 'PaginatedList',
        'first': f'/creditors/4294967296/accounts/1/entries?prev={next_entry_id}'
    }
    assert data['account'] == {'uri': '/creditors/4294967296/accounts/1/'}


def test_ledger_entries_list(ledger_entries, client, current_ts):
    r = client.get('/creditors/4294967299/accounts/1/entries?prev=100')
    assert r.status_code == 404 or r.get_json()['items'] == []

    r = client.get('/creditors/4294967296/accounts/1111/entries?prev=100')
    assert r.status_code == 404 or r.get_json()['items'] == []

    items = _get_all_pages(client, '/creditors/4294967296/accounts/1/entries?prev=1000000000000000', page_type='LedgerEntriesPage')
    assert len(items) == 3
    first_entry_id = items[2]['entryId']
    assert items == [
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/4294967296/accounts/1/ledger'},
            'addedAt': current_ts.isoformat(),
            'entryId': first_entry_id + 2,
            'aquiredAmount': 200,
            'principal': 350,
            'transfer': {'uri': '/creditors/4294967296/accounts/1/transfers/0-2'},
        },
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/4294967296/accounts/1/ledger'},
            'addedAt': current_ts.isoformat(),
            'entryId': first_entry_id + 1,
            'aquiredAmount': 50,
            'principal': 150,
        },
        {
            'type': 'LedgerEntry',
            'ledger': {'uri': '/creditors/4294967296/accounts/1/ledger'},
            'addedAt': current_ts.isoformat(),
            'entryId': first_entry_id,
            'aquiredAmount': 100,
            'principal': 100,
            'transfer': {'uri': '/creditors/4294967296/accounts/1/transfers/0-1'},
        },
    ]

    items = _get_all_pages(client, f'/creditors/4294967296/accounts/1/entries?prev={first_entry_id}',
                           page_type='LedgerEntriesPage')
    assert len(items) == 0

    items = _get_all_pages(client, f'/creditors/4294967296/accounts/1/entries?prev=1000000000000000&stop={first_entry_id}',
                           page_type='LedgerEntriesPage')
    assert len(items) == 2

    items = _get_all_pages(client, f'/creditors/4294967296/accounts/1/entries?prev={first_entry_id + 2}&stop={first_entry_id}',
                           page_type='LedgerEntriesPage')
    assert len(items) == 1

    items = _get_all_pages(client, f'/creditors/4294967296/accounts/1/entries?prev={first_entry_id + 1}&stop=1000000000000000',
                           page_type='LedgerEntriesPage')
    assert len(items) == 0

    items = _get_all_pages(client, f'/creditors/4294967296/accounts/1/entries?prev={first_entry_id + 1}&stop={first_entry_id + 1}',
                           page_type='LedgerEntriesPage')
    assert len(items) == 0

    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    ledger_latest_update_id = p.get_account_ledger(4294967296, 1).ledger_latest_update_id
    account_uid = p.get_account(4294967296, 1).latest_update_id
    assert len(entries) == 4
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('data'))
            for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid, None),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2, None),
        (
            'AccountLedger',
            '/creditors/4294967296/accounts/1/ledger',
            ledger_latest_update_id - 1,
            {
                'principal': 100,
                'nextEntryId': first_entry_id + 1,
                'firstPage': f'/creditors/4294967296/accounts/1/entries?prev={first_entry_id + 1}',
            },
        ),
        (
            'AccountLedger',
            '/creditors/4294967296/accounts/1/ledger',
            ledger_latest_update_id,
            {
                'principal': 350,
                'nextEntryId': first_entry_id + 3,
                'firstPage': f'/creditors/4294967296/accounts/1/entries?prev={first_entry_id + 3}',
            },
        ),
    ]


def test_get_committed_transfer(client, account, current_ts):
    params = {
        'debtor_id': 1,
        'creditor_id': 4294967296,
        'creation_date': date(1970, 1, 1),
        'transfer_number': 1,
        'coordinator_type': 'interest',
        'sender': '4294967297',
        'recipient': '4294967296',
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

    r = client.get('/creditors/4294967296/accounts/1/transfers/0-1')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'CommittedTransfer'
    assert data['uri'] == '/creditors/4294967296/accounts/1/transfers/0-1'
    assert data['committedAt'] == current_ts.isoformat()
    assert data['rationale'] == 'interest'
    assert data['noteFormat'] == 'json'
    assert data['note'] == '{"message": "test"}'
    assert data['account']['uri'] == '/creditors/4294967296/accounts/1/'
    assert data['acquiredAmount'] == 100
    assert data['sender']['uri'] == 'swpt:1/4294967297'
    assert data['recipient']['uri'] == 'swpt:1/4294967296'

    r = client.get('/creditors/4294967296/accounts/1/transfers/1-1')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/transfers/11111111111111111111111111111-1')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/transfers/INVALID')
    assert r.status_code == 404

    r = client.get('/creditors/4294967296/accounts/1/transfers/1-0')
    assert r.status_code == 404


def test_create_transfer(client, account):
    p.process_pending_log_entries(4294967296)

    request_data = {
        'type': 'TransferCreationRequest',
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
        'recipient': {'uri': 'swpt:1/4294967299'},
        'amount': 1000,
        'noteFormat': 'json',
        'note': '{"message": "test"}',
        'options': {
            'type': 'TransferOptions',
            'minInterestRate': -10,
            'deadline': '2009-08-24T14:15:22+00:00',
            'lockedAmount': 1000,
        },
        'pin': '1234',
    }

    r = client.post('/creditors/4294967299/transfers/', json=request_data)
    assert r.status_code == 404

    r = client.post('/creditors/4294967296/transfers/', json=request_data)
    assert r.status_code == 201
    assert r.headers['location'] == \
        'http://localhost/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000'
    p.process_pending_log_entries(4294967296)

    r = client.get('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 200
    data = r.get_json()
    assert data['type'] == 'Transfer'
    assert data['recipient']['uri'] == 'swpt:1/4294967299'
    assert data['amount'] == 1000
    assert data['note'] == '{"message": "test"}'
    assert data['noteFormat'] == 'json'
    assert datetime.fromisoformat(data['initiatedAt'])
    assert data['transferUuid'] == '123e4567-e89b-12d3-a456-426655440000'
    assert data['latestUpdateId'] == 1
    assert datetime.fromisoformat(data['latestUpdateAt'])
    assert 'result' not in data
    assert data['transfersList']['uri'] == '/creditors/4294967296/transfers-list'
    assert datetime.fromisoformat(data['checkupAt'])
    assert data['options'] == {
        'type': 'TransferOptions',
        'minInterestRate': -10.0,
        'deadline': '2009-08-24T14:15:22+00:00',
        'lockedAmount': 1000,
    }

    r = client.post('/creditors/4294967296/transfers/', json=request_data)
    assert r.status_code == 303

    r = client.post('/creditors/4294967296/transfers/', json={**request_data, 'amount': 999})
    assert r.status_code == 409

    r = client.post('/creditors/4294967296/transfers/', json={**request_data, 'recipient': {'uri': 'INVALID'}})
    assert r.status_code == 422
    assert r.get_json()['errors']['json']['recipient']['uri'] == ['The URI can not be recognized.']

    r = client.post('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440001', json={})
    assert r.status_code == 404

    r = client.post('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000', json={})
    assert r.status_code == 200
    r = client.post('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000', json={})
    assert r.status_code == 200
    data = r.get_json()
    assert data['result']['error']['errorCode'] == m.SC_CANCELED_BY_THE_SENDER
    assert data['latestUpdateId'] == 2
    p.process_pending_log_entries(4294967296)

    r = client.delete('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440001')
    assert r.status_code == 204

    r = client.delete('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 204
    p.process_pending_log_entries(4294967296)

    r = client.get('/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000')
    assert r.status_code == 404

    account_uid = p.get_account(4294967296, 1).latest_update_id
    entries = _get_all_pages(client, '/creditors/4294967296/log', page_type='LogEntriesPage', streaming=True)
    assert len(entries) == 7
    assert [(e['objectType'], e['object']['uri'], e.get('objectUpdateId'), e.get('deleted', False))
            for e in entries] == [
        ('Account', '/creditors/4294967296/accounts/1/', account_uid, False),
        ('AccountsList', '/creditors/4294967296/accounts-list', 2, False),
        ('Transfer', '/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000', 1, False),
        ('TransfersList', '/creditors/4294967296/transfers-list', 2, False),
        ('Transfer', '/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000', 2, False),
        ('Transfer', '/creditors/4294967296/transfers/123e4567-e89b-12d3-a456-426655440000', None, True),
        ('TransfersList', '/creditors/4294967296/transfers-list', 3, False),
    ]

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'on',
        'newPin': '5678',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    for i in range(2):
        r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
        assert r.status_code == 403

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'on'

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'blocked'

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.post('/creditors/4294967298/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 404


def test_unauthorized_creditor_id(creditor, client):
    r = client.get('/creditors/4294967296/')
    assert r.status_code == 200

    r = client.get('/creditors/4294967296/', headers={'X-Swpt-User-Id': 'creditors-supervisor'})
    assert r.status_code == 200

    r = client.get('/creditors/4294967296/', headers={'X-Swpt-User-Id': 'creditors:4294967296'})
    assert r.status_code == 200

    r = client.get('/creditors/4294967296/', headers={'X-Swpt-User-Id': 'creditors:4294967298'})
    assert r.status_code == 403

    r = client.get('/creditors/18446744073709551615/', headers={'X-Swpt-User-Id': 'creditors:18446744073709551615'})
    assert r.status_code == 403

    r = client.get('/creditors/18446744073709551615/', headers={'X-Swpt-User-Id': 'INVALID_USER_ID'})
    assert r.status_code == 403

    with pytest.raises(ValueError):
        r = client.get('/creditors/18446744073709551615/', headers={'X-Swpt-User-Id': 'creditors:18446744073709551616'})


def test_pin_cfa_reset(client, creditor, account):
    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'on',
        'newPin': '1234',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'on',
        'newPin': '1234',
        'latestUpdateId': 2,
    })
    assert r.status_code == 409

    request_data = {
        'type': 'TransferCreationRequest',
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
        'recipient': {'uri': 'swpt:1/4294967299'},
        'amount': 1000,
        'noteFormat': 'json',
        'note': '{"message": "test"}',
        'options': {
            'type': 'TransferOptions',
            'minInterestRate': -10,
            'deadline': '2009-08-24T14:15:22+00:00',
            'lockedAmount': 1000,
        },
        'pin': '5678',
    }

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.post(
        '/creditors/4294967296/transfers/',
        headers={'X-Swpt-Require-Pin': 'true'},
        json={**request_data, 'pin': '1234'},
    )
    assert r.status_code == 201

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'on'

    r = client.post('/creditors/4294967296/transfers/', headers={'X-Swpt-Require-Pin': 'true'}, json=request_data)
    assert r.status_code == 403

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'blocked'


@pytest.fixture(params=[10.0, -10.0])
def reset_days(request):
    return request.param


def test_pin_afa_reset(app, client, creditor, account, reset_days):
    app.config['APP_PIN_FAILURES_RESET_DAYS'] = reset_days

    r = client.patch('/creditors/4294967296/pin', json={
        'status': 'on',
        'newPin': '1234',
        'latestUpdateId': 2,
    })
    assert r.status_code == 200

    request_data = {
        'type': 'TransferCreationRequest',
        'transferUuid': '123e4567-e89b-12d3-a456-426655440000',
        'recipient': {'uri': 'swpt:1/4294967299'},
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

    correct_pin = {**request_data, 'pin': '1234'}
    incorrect_pin = {**request_data, 'pin': '5678'}
    headers = {'X-Swpt-Require-Pin': 'true'}

    for i in range(9):
        r = client.post('/creditors/4294967296/transfers/', headers=headers, json=incorrect_pin)
        assert r.status_code == 403

        r = client.post('/creditors/4294967296/transfers/', headers=headers, json=correct_pin)
        assert r.status_code in [201, 303]

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'on'

    r = client.post('/creditors/4294967296/transfers/', headers=headers, json=incorrect_pin)
    assert r.status_code == 403

    r = client.get('/creditors/4294967296/pin')
    assert r.status_code == 200
    data = r.get_json()
    assert data['status'] == 'blocked' if reset_days >= 1 else 'on'
