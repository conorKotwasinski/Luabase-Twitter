import json

import pytest
from luapy.app import create_app
from luapy.db import SQLALCHEMY_ENGINE_OPTIONS


sqlite_engine_options = SQLALCHEMY_ENGINE_OPTIONS
del sqlite_engine_options['pool_size']


CREATE_JOB_TABLE = "".join([s.strip() for s in """
CREATE TABLE public.jobs (
    id integer PRIMARY KEY NOT NULL,
    uuid character varying DEFAULT (lower(hex(randomblob(16)))),
    created_at timestamp with time zone DEFAULT (STRFTIME('%s')) NOT NULL,
    updated_at timestamp with time zone DEFAULT (STRFTIME('%s')) NOT NULL,
    status character varying NOT NULL,
    type character varying,
    details jsonb
);
""".split("\n")])


jobs = [
    {
        "type": "PYTEST_ONLY",
        "status": "pending",
        "details": json.dumps({"end": 27729499, "type": "PYTEST_ONLY", "start": 27729000, "maxRunning": 45})
    },
    {
        "type": "PYTEST_ONLY",
        "status": "running",
        "details": json.dumps({"end": 27729499, "type": "PYTEST_ONLY", "start": 27729000, "maxRunning": 77})
    },
    {
        "type": "PYTEST_ONLY",
        "status": "failed",
        "details": json.dumps({"end": 27729499, "type": "PYTEST_ONLY", "start": 27729000, "maxRunning": 23})
    }
]


def insert_jobs(jobs):
    columns = ", ".join([f"`{c}`" for c in jobs[0].keys()])
    values = ", ".join([
        "(" + ", ".join([f"'{c}'" for c in job.values()]) + ")"
        for job in jobs
    ])
    return "INSERT INTO jobs ({columns}) VALUES {values};".format(
        columns=columns,
        values=values
    )


@pytest.fixture(scope='module')
def client_db():
    app, db = create_app(db_options={'engine_options': sqlite_engine_options})
    app.config.update({
        'TESTING': True,
        'SQLALCHEMY_DATABASE_URI': 'sqlite://',
    })

    with app.app_context():
        with db.engine.connect() as c:
            c.execute("ATTACH DATABASE ':memory:' AS public;")
            c.execute(CREATE_JOB_TABLE)
            c.execute(insert_jobs(jobs))

        with app.test_client() as client:
            yield client, db


@pytest.fixture(scope='module')
def client(client_db):
    yield client_db[0]
