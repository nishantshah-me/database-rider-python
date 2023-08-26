from pathlib import Path

import pytest
from peewee import SqliteDatabase

from lib.database import DatabaseExecutor
from lib.handler import DataSetHandler
from lib.loader import DelegatingDataSetLoader, JsonDataSetLoader, YamlDataSetLoader
from lib.matcher import DataSetMatcher
from lib.model import DataSetConfig


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
    database.execute_sql("DROP TABLE IF EXISTS statuses;")
    database.execute_sql("""
                CREATE TABLE statuses (
                    id INTEGER NOT NULL PRIMARY KEY,
                    status TEXT NOT NULL,
                    enabled BOOL NOT NULL
                );
            """)
    yield database
    database.execute_sql("DROP TABLE IF EXISTS users;")
    database.execute_sql("DROP TABLE IF EXISTS statuses;")


@pytest.fixture(autouse=True)
def database_executor(database):
    database_executor = DatabaseExecutor(database)
    database_executor.init()
    yield database_executor


@pytest.fixture(autouse=True)
def dataset_matcher(database_executor):
    matcher = DataSetMatcher(database_executor)
    yield matcher


@pytest.fixture(autouse=True)
def dataset_loader():
    dataset_loader = DelegatingDataSetLoader({"json": JsonDataSetLoader(), "yaml": YamlDataSetLoader()})
    yield dataset_loader


def test_handler_skip_everything(database, dataset_matcher, dataset_loader, database_executor):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    config = DataSetConfig(
        dataset_paths=None,
        dataset_providers=None,
        dataset_variables=None,
        cleanup_before=False,
        cleanup_after=False,
        cleanup_tables=None,
        execute_scripts_before=None,
        execute_statements_before=None,
        execute_statements_after=None,
        execute_scripts_after=None,
        expected_dataset_paths=None,
        expected_dataset_providers=None,
        expected_dataset_matchers=None
    )
    dataset_handler = DataSetHandler(config, dataset_loader, database_executor, dataset_matcher, Path(__file__).parent)
    dataset_handler.execute_before()
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 1
    dataset_handler.execute_after()
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 1


def test_handler(database, dataset_matcher, dataset_loader, database_executor):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    database.execute_sql("INSERT INTO statuses (id, status, enabled) VALUES (1, 'Active', FALSE);")
    database.execute_sql("INSERT INTO statuses (id, status, enabled) VALUES (2, 'Inactive', FALSE);")
    config = DataSetConfig(
        dataset_paths=["datasets/handler.yaml"],
        dataset_providers=[lambda: {"users": [{"id": 3, "name": "{trevor_name}"}]}],
        dataset_variables={"trevor_name": "Trevor", "bob_id": 2},
        cleanup_before=True,
        cleanup_after=True,
        cleanup_tables=["users"],
        execute_scripts_before=["sql/before.sql"],
        execute_statements_before=["INSERT INTO statuses (id, status, enabled) VALUES (3, 'Unknown', FALSE);"],
        execute_statements_after=["DELETE FROM statuses WHERE id = 3;"],
        execute_scripts_after=["sql/after.sql"],
        expected_dataset_paths=["expected_datasets/handler.yaml"],
        expected_dataset_providers=[lambda: {"statuses": [
            {"id": 1, "status": "Active", "enabled": True},
            {"id": 2, "status": "Inactive", "enabled": True},
            {"id": 3, "status": "Unknown", "enabled": False}
        ]}],
        expected_dataset_matchers={"id_matcher": lambda v, r: v == 2 if r["name"] == "Bob" else v == 3}
    )
    dataset_handler = DataSetHandler(config, dataset_loader, database_executor, dataset_matcher, Path(__file__).parent)
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 1
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 2
    dataset_handler.execute_before()
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 2
    assert database.execute_sql("SELECT name FROM users WHERE id=2").fetchone()[0] == "Bob"
    assert database.execute_sql("SELECT name FROM users WHERE id=3").fetchone()[0] == "Trevor"
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 3
    assert database.execute_sql("SELECT status, enabled FROM statuses WHERE id=1").fetchone() == ("Active", True)
    assert database.execute_sql("SELECT status, enabled FROM statuses WHERE id=2").fetchone() == ("Inactive", True)
    assert database.execute_sql("SELECT status, enabled FROM statuses WHERE id=3").fetchone() == ("Unknown", False)
    dataset_handler.execute_after()
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 0
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 2
    assert database.execute_sql("SELECT status, enabled FROM statuses WHERE id=1").fetchone() == ("Active", False)
    assert database.execute_sql("SELECT status, enabled FROM statuses WHERE id=2").fetchone() == ("Inactive", False)


def test_handler_without_variables(database, dataset_matcher, dataset_loader, database_executor):
    database.execute_sql("INSERT INTO users (id, name) VALUES (1, 'John');")
    config = DataSetConfig(
        dataset_paths=None,
        dataset_providers=[lambda: {"users": [{"id": 2, "name": "{bob_name}"}]}],
        dataset_variables=None,
        cleanup_before=True,
        cleanup_after=True,
        cleanup_tables=None,
        execute_scripts_before=None,
        execute_statements_before=None,
        execute_statements_after=None,
        execute_scripts_after=None,
        expected_dataset_paths=None,
        expected_dataset_providers=None,
        expected_dataset_matchers=None
    )
    dataset_handler = DataSetHandler(config, dataset_loader, database_executor, dataset_matcher, Path(__file__).parent)
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 1
    assert database.execute_sql("SELECT COUNT(1) FROM statuses").fetchone()[0] == 0
    dataset_handler.execute_before()
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 1
    assert database.execute_sql("SELECT name FROM users WHERE id=2").fetchone()[0] == "{bob_name}"
    dataset_handler.execute_after()
    assert database.execute_sql("SELECT COUNT(1) FROM users").fetchone()[0] == 0
