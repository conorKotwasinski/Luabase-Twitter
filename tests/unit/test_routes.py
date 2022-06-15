import json

import pytest
import sqlalchemy
from sqlalchemy import sql


def test_db(client_db):
    client, db = client_db

    with db.engine.connect() as c:
        r = c.execute("select id, type, status, json_extract(details, '$.maxRunning') as maxRunning from jobs;")
        rows = r.mappings().all()
        row1, row2, row3 = rows[:3]

        for i, row in enumerate([row1, row2, row3]):
            assert row['type'] == "PYTEST_ONLY"
            assert row['id'] == i+1

        assert row1['status'] == 'pending'
        assert row1['maxRunning'] == 45


        assert row2['status'] == 'running'
        assert row2['maxRunning'] == 77

        assert row3['status'] == 'failed'
        assert row3['maxRunning'] == 23


@pytest.mark.parametrize("method", ("get", "post"))
def test_ping(client_db, method):
    client, db = client_db
    response = getattr(client, method)('/ping')
    assert response.data == b'{"ok": true, "name": "World"}'


@pytest.mark.parametrize("method", ("get", "post"))
def test_ping_sql(client, method):
    response = getattr(client, method)('/ping_sql')
    assert response.text == '{"max_id": 3, "min_id": 1, "count": 3, "running": 1}'


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


def test_run_job_error(client_db, mocker):
    client, db = client_db

    # Because of different dialects sqlite != postgres we need to mock the
    # update row function.
    def updateSqlite(engine, d):
        with engine.connect() as con:
            try:
                sql = '''
                UPDATE public.jobs
                SET "status" = :status,
                "updated_at" = datetime()
                WHERE id = :id
                '''
                statement = sqlalchemy.sql.text(sql)
                row = con.execute(statement, **d)
                return {'ok': True, 'row': d['id']}
            except Exception as e:
                return {'ok': False, 'error': e}
    mocker.patch('luapy.app.pgu.updateJobStatus', side_effect=updateSqlite)

    response = client.post('/run_job', json={
        'id': 2,
        'type': 'PYTEST_ONLY',
        'error': True,
    })

    assert response.status_code == 500
    data = json.loads(response.data)
    assert "error" in data
    assert data["error"] == "HandledError"
    with db.engine.connect() as c:
        q = sql.text("select status from jobs where id = :id;")
        r = c.execute(q, {"id": 2})
        row = r.mappings().fetchone()
        assert row['status'] == 'failed'
