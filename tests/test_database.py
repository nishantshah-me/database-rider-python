import pytest
from peewee import SqliteDatabase

from database_rider.database import DatabaseExecutor


@pytest.fixture(autouse=True)
def database():
    database = SqliteDatabase("test_database.db")
    database.execute_sql("DROP TABLE IF EXISTS users;")
    database.execute_sql("""
            CREATE TABLE users (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            );
        """)
    database.execute_sql("DROP TABLE IF EXISTS statuses;")
    database.execute_sql("""
                CREATE TABLE statuses (
                    id INTEGER NOT NULL PRIMARY KEY,
                    status TEXT NOT NULL,
                    user_id INTEGER REFERENCES users(id)
                );
            """)
    yield database
    database.execute_sql("DROP TABLE IF EXISTS statuses;")
    database.execute_sql("DROP TABLE IF EXISTS users;")


def test_fetch_all(database):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    database.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")

    database_executor = DatabaseExecutor(database)
    database_executor.init()
    result = database_executor.fetch_all('users')
    assert len(result) == 2
    assert {rec["name"] for rec in result} == {"John", "Bob"}


def test_cleanup_tables_implicit(database):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    database.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
    database.execute_sql("INSERT INTO statuses (id, status, user_id) VALUES (1, 'Active', 1);")

    database_executor = DatabaseExecutor(database)
    database_executor.init()
    database_executor.cleanup_tables()
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 0
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 0


def test_cleanup_tables_explicit(database):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    database.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
    database.execute_sql("INSERT INTO statuses (id, status, user_id) VALUES (1, 'Active', 1);")

    database_executor = DatabaseExecutor(database)
    database_executor.init()
    database_executor.cleanup_tables(["statuses"])
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 0
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 2


def test_insert_records(database):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    database.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
    database.execute_sql("INSERT INTO statuses (id, status, user_id) VALUES (1, 'Active', 1);")

    database_executor = DatabaseExecutor(database)
    database_executor.init()
    database_executor.insert_records({"statuses": [{"id": 2, "status": "Inactive", "user_id": 1}]})
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 2
    assert database.execute_sql("SELECT status FROM statuses WHERE id = 2 AND user_id = 1").fetchone()[0] == "Inactive"
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 2


def test_execute_query(database):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    database.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Bob');")
    database.execute_sql("INSERT INTO statuses (id, status, user_id) VALUES (1, 'Active', 1);")

    database_executor = DatabaseExecutor(database)
    database_executor.init()
    database_executor.execute_query("DELETE FROM users WHERE id = 2")
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 1
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 1
    assert database.execute_sql("SELECT name FROM users WHERE id = 1").fetchone()[0] == "John"
