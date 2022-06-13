import json

import pytest
import sqlalchemy


@pytest.mark.parametrize("method", ("get", "post"))
def test_ping(client, method):
    response = getattr(client, method)('/ping')
    assert b'{"ok": true, "name": "World"}' == response.data


@pytest.mark.parametrize("method", ("get", "post"))
def test_ping_sql(client, method):
    with pytest.raises(sqlalchemy.exc.OperationalError) as e:
        response = getattr(client, method)('/ping_sql')
    assert str(e.value.orig) == 'no such table: public.jobs'


@pytest.mark.parametrize("method", ("get", "post"))
def test_test_secret(client, method):
    response = getattr(client, method)('/test_secret')
    assert response.status_code == 200
    assert response.data == b'{"ok": true, "test_from_cloud_run": "it_also_works"}'


def test_run_job(client):
    response = client.post('/run_job', json={
        'type': 'PYTEST_ONLY'
    })
    assert response.status_code == 200
    assert response.data == b'{"ok": true, "data": "Hello pytest!"}'


def test_run_job_error(client):
    response = client.post('/run_job', json={
        'type': 'PYTEST_ONLY',
        'error': True,
    })
    assert response.status_code == 500
    data = json.loads(response.data)
    assert "error" in data
    assert data["error"] == "HandledError"
