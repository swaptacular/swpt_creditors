import pytest
from datetime import date
import iso8601
from swpt_creditors import procedures as p


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def debtor(db_session):
    return p.lock_or_create_creditor(2)


def test_create_creditor(client):
    r = client.get('/creditors/2222/')
    assert r.status_code == 404

    r = client.post('/creditors/2/', json={})
    assert r.status_code == 201
    assert r.headers['Location'] == 'http://example.com/creditors/2/'
    data = r.get_json()
    assert isinstance(iso8601.parse_date(data['createdOn']).date(), date)
    assert data['isActive'] is False
    assert data['type'] == 'Creditor'
    assert data['uri'] == 'http://example.com/creditors/2/'

    r = client.post('/creditors/2/', json={})
    assert r.status_code == 409

    r = client.get('/creditors/2/')
    assert r.status_code == 200
    assert 'max-age' in r.headers['Cache-Control']
    data = r.get_json()
    assert isinstance(iso8601.parse_date(data['createdOn']).date(), date)
    assert data['isActive'] is False
    assert data['type'] == 'Creditor'
    assert data['uri'] == 'http://example.com/creditors/2/'
