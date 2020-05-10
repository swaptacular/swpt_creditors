import pytest
from datetime import date
import iso8601
from swpt_creditors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def creditor(db_session):
    return p.lock_or_create_creditor(2)


def test_create_creditor(client):
    r = client.get('/creditors/2222/')
    assert r.status_code == 404

    r = client.post('/creditors/2/', json={})
    assert r.status_code == 201
    assert r.headers['Location'] == 'http://example.com/creditors/2/'
    data = r.get_json()
    assert data['active'] is False
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'

    r = client.post('/creditors/2/', json={})
    assert r.status_code == 409

    r = client.get('/creditors/2/')
    assert r.status_code == 200
    assert 'max-age' in r.headers['Cache-Control']
    data = r.get_json()
    assert data['active'] is False
    assert data['type'] == 'Creditor'
    assert data['uri'] == '/creditors/2/'


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
    assert log['forthcoming'] == '/creditors/2/log?prev=0'
    assert log['itemsType'] == 'LogEntry'
    dt = data['transfers']
    assert dt['type'] == 'PaginatedList'
    assert dt['first'] == '/creditors/2/transfers/'
    assert dt['totalItems'] == 0
    assert dt['itemsType'] == 'string'
    ar = data['accounts']
    assert ar['type'] == 'PaginatedList'
    assert ar['first'] == '/creditors/2/accounts/'
    assert ar['totalItems'] == 0
    assert ar['itemsType'] == 'string'
