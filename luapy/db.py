from flask_sqlalchemy import SQLAlchemy


SQLALCHEMY_SESSION_OPTIONS = {
    'autocommit': True,
    'pool_size': 10,
    'pool_recycle': 60,
    "max_overflow": 2,
    'pool_pre_ping': True,
}

SQLALCHEMY_ENGINE_OPTIONS = {
    'pool_size': 10,
    'pool_recycle': 60,
    'pool_pre_ping': True
}


def create_db(session_options=SQLALCHEMY_SESSION_OPTIONS, engine_options=SQLALCHEMY_ENGINE_OPTIONS):
    return SQLAlchemy(session_options=session_options, engine_options=engine_options)
