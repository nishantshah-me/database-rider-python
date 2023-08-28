import pytest
from _pytest.outcomes import fail
from peewee import SqliteDatabase

from dbrider.database import DatabaseExecutor
from dbrider.matcher import DataSetMatcher


@pytest.fixture(autouse=True)
def database():
    database = SqliteDatabase("test_matcher.db")
    database.execute_sql("DROP TABLE IF EXISTS users;")
    database.execute_sql("""
            CREATE TABLE users (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL
            );
        """)
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    yield database
    database.execute_sql("DROP TABLE IF EXISTS users;")


@pytest.fixture(autouse=True)
def dataset_matcher(database):
    database_executor = DatabaseExecutor(database)
    database_executor.init()
    matcher = DataSetMatcher(database_executor)
    yield matcher


def test_matcher_simple(dataset_matcher):
    dataset_matcher.matches({"users": [{"id": 1, "name": "John"}]}, None)


def test_matcher_several_reecords(database, dataset_matcher):
    database.execute_sql("INSERT INTO users (id, name) VALUES (2, 'Trevor');")
    dataset_matcher.matches({"users": [{"id": 2, "name": "Trevor"}, {"id": 1, "name": "John"}]}, None)


def test_matcher_scripted(dataset_matcher):
    dataset_matcher.matches({"users": [{"id": "matcher: id_matcher", "name": "John"}]},
                            {"id_matcher": lambda val, rec: val == 1 if rec["name"] == "John" else val == 2})


def test_matcher_record_count(dataset_matcher):
    try:
        dataset_matcher.matches({"users": [{"id": 1, "name": "John"}, {"id": 2, "name": "Bob"}]}, None)
        fail()
    except ValueError:
        pass


def test_matcher_simple_record_doesnt_match(dataset_matcher):
    try:
        dataset_matcher.matches({"users": [{"id": 1, "name": "Bob"}]}, None)
        fail()
    except ValueError:
        pass


def test_matcher_scripted_record_doesnt_match(dataset_matcher):
    try:
        dataset_matcher.matches({"users": [{"id": 1, "name": "matcher:name_matcher"}]},
                                {"name_matcher": lambda v, r: v == "Bob"})
        fail()
    except ValueError:
        pass
