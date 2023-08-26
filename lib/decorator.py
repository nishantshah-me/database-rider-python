import functools
from typing import Any, Callable
from pathlib import Path

from . import create_dataset_handler


def dataset(dataset_paths: list[str] | None = None,
            dataset_providers: list[Callable[[], dict[str, list[dict[str, Any]]]]] | None = None,
            dataset_variables: dict[str, Any] | None = None,
            cleanup_before: bool = True, cleanup_after: bool = True, cleanup_tables: list[str] | None = None,
            execute_scripts_before: list[str] = None, execute_statements_before: list[str] = None,
            execute_scripts_after: list[str] = None, execute_statements_after: list[str] = None,
            expected_dataset_paths: list[str] | None = None,
            expected_dataset_providers: list[Callable[[], dict[str, list[dict[str, Any]]]]] | None = None,
            expected_dataset_matchers: dict[str, Callable[[Any, dict[str, Any]], bool]] | None = None):
    def decorator_dataset(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            path = Path(func.__globals__['__file__']).parent
            handler = create_dataset_handler(
                dataset_paths, dataset_providers, dataset_variables, cleanup_before, cleanup_after, cleanup_tables,
                execute_scripts_before, execute_statements_before, execute_scripts_after, execute_statements_after,
                expected_dataset_paths, expected_dataset_providers, expected_dataset_matchers, path)
            try:
                handler.execute_before()
                value = func(*args, **kwargs)
            finally:
                handler.execute_after()
            return value
        return wrapper
    return decorator_dataset
