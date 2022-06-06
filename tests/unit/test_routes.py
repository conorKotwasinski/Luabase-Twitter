import pytest


@pytest.mark.parametrize("method", ("get", "post"))
def test_ping(client, method):
    response = getattr(client, method)('/ping')
    assert b'{"ok": true, "name": "World"}' == response.data

def test_ping_sql():
    assert 0 == 0

def test_test_secret():
    pass

def test_run_job():
    pass
