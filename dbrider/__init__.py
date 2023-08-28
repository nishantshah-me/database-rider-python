from contextvars import ContextVar
from pathlib import Path
from typing import Callable, Any

from peewee import Database

from dbrider.database import DatabaseExecutor
from dbrider.handler import DataSetHandler
from dbrider.loader import DataSetLoader, YamlDataSetLoader, DelegatingDataSetLoader, JsonDataSetLoader
from dbrider.matcher import DataSetMatcher
from dbrider.model import DataSetConfig

db_var = ContextVar('db')
db_executor_var = ContextVar('database_executor')
dataset_matcher_var = ContextVar('dataset_matcher')
dataset_loader_var = ContextVar('dataset_loader')


def setup_db_rider(
        database: Database,
        database_executor: DatabaseExecutor = None,
        dataset_matcher: DataSetMatcher = None,
        dataset_loader: DataSetLoader = None):
    db_var.set(database)
    db_executor_var.set(database_executor or DatabaseExecutor(database))
    dataset_matcher_var.set(dataset_matcher or DataSetMatcher(db_executor_var.get()))
    dataset_loader_var.set(dataset_loader or _default_dataset_loader())


def _default_dataset_loader() -> DataSetLoader:
    yaml_dataset_loader = YamlDataSetLoader()
    return DelegatingDataSetLoader({
        "json": JsonDataSetLoader(),
        "yaml": yaml_dataset_loader,
        "yml": yaml_dataset_loader
    })


def create_dataset_handler(
        dataset_paths: list[str] | None = None,
        dataset_providers: list[Callable[[], dict[str, list[dict[str, Any]]]]] | None = None,
        dataset_variables: dict[str, Any] | None = None,
        cleanup_before: bool = True, cleanup_after: bool = True, cleanup_tables: list[str] | None = None,
        execute_scripts_before: list[str] = None, execute_statements_before: list[str] = None,
        execute_scripts_after: list[str] = None, execute_statements_after: list[str] = None,
        expected_dataset_paths: list[str] | None = None,
        expected_dataset_providers: list[Callable[[], dict[str, list[dict[str, Any]]]]] | None = None,
        expected_dataset_matchers: dict[str, Callable[[Any, dict[str, Any]], bool]] | None = None,
        path: Path = Path.cwd()):
    config = DataSetConfig(
        dataset_paths=dataset_paths,
        dataset_providers=dataset_providers,
        dataset_variables=dataset_variables,
        cleanup_before=cleanup_before,
        cleanup_after=cleanup_after,
        cleanup_tables=cleanup_tables,
        execute_scripts_before=execute_scripts_before,
        execute_statements_before=execute_statements_before,
        execute_scripts_after=execute_scripts_after,
        execute_statements_after=execute_statements_after,
        expected_dataset_paths=expected_dataset_paths,
        expected_dataset_providers=expected_dataset_providers,
        expected_dataset_matchers=expected_dataset_matchers
    )
    return DataSetHandler(config, dataset_loader_var.get(), db_executor_var.get(), dataset_matcher_var.get(), path)
