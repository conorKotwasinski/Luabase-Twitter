import pytest
from luapy.app import create_app
from luapy.db import SQLALCHEMY_ENGINE_OPTIONS


sqlite_engine_options = SQLALCHEMY_ENGINE_OPTIONS
del sqlite_engine_options['pool_size']


@pytest.fixture
def client():
    app = create_app(db_options={'engine_options': sqlite_engine_options})
    app.config.update({
        'TESTING': True,
        'SQLALCHEMY_DATABASE_URI': 'sqlite://',
    })

    with app.test_client() as client:
        yield client

