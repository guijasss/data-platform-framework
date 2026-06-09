from pathlib import Path

import pytest

from sqlalchemy import text
from dotenv import load_dotenv

ROOT_DIR = Path(__file__).resolve().parents[2]

load_dotenv(ROOT_DIR / ".env.test")

from src.database.connection import make_engine


def setup_database() -> None:
    engine = make_engine(kind="sqlalchemy")

    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS raw_test CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS silver_test CASCADE"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_test"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS silver_test"))

        conn.execute(text("""
            CREATE TABLE raw_test.users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                processed_at TIMESTAMP NOT NULL
            )
        """))

        conn.execute(text("""
            CREATE TABLE silver_test.users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                processed_at TIMESTAMP NOT NULL
            )
        """))

        conn.execute(text("""
            INSERT INTO raw_test.users (
                id,
                name,
                processed_at
            )
            VALUES
                (1, 'Alice', '2024-01-01 10:00:00'),
                (2, 'Bob', '2024-01-01 13:00:00'),
                (3, 'Carol', '2024-01-02 09:00:00')
        """))

        conn.execute(text("""
            INSERT INTO silver_test.users (
                id,
                name,
                processed_at
            )
            VALUES
                (1, 'Alice', '2024-01-01 12:00:00')
        """))


def teardown_database() -> None:
    engine = make_engine(kind="sqlalchemy")

    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS raw_test CASCADE"))
        conn.execute(text("DROP SCHEMA IF EXISTS silver_test CASCADE"))


@pytest.fixture(scope="function")
def database():
    teardown_database()
    setup_database()

    yield

    teardown_database()
