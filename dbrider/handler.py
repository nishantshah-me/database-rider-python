from pathlib import Path
from typing import Any, Callable

from .database import DatabaseExecutor
from .loader import DataSetLoader
from .model import DataSetConfig
from .matcher import DataSetMatcher


class DataSetHandler(object):
    def __init__(self, dataset_config: DataSetConfig, dataset_loader: DataSetLoader,
                 database_executor: DatabaseExecutor, dataset_matcher: DataSetMatcher,
                 relative_path_starting_point: Path):

        self.dataset_config = dataset_config
        self.dataset_loader = dataset_loader
        self.database_executor = database_executor
        self.dataset_matcher = dataset_matcher
        self.relative_path_starting_point = relative_path_starting_point

    def execute_before(self):
        table_name_to_records = None
        if self.dataset_config.dataset_paths or self.dataset_config.dataset_providers:
            table_name_to_records = self._process_datasets(self.dataset_config.dataset_paths,
                                                           self.dataset_config.dataset_providers)
            if self.dataset_config.dataset_variables:
                self._interpolate_variables(table_name_to_records)

        self.database_executor.init()

        if self.dataset_config.cleanup_before:
            self.database_executor.cleanup_tables(self.dataset_config.cleanup_tables)

        if self.dataset_config.execute_scripts_before:
            self._execute_scripts(self.dataset_config.execute_scripts_before)

        if self.dataset_config.execute_statements_before:
            self._execute_statements(self.dataset_config.execute_statements_before)

        if table_name_to_records:
            self.database_executor.insert_records(table_name_to_records)

    def execute_after(self):
        try:
            if self.dataset_config.expected_dataset_paths or self.dataset_config.expected_dataset_providers:
                expected_dataset_table_name_to_records = self._process_datasets(
                                                                        self.dataset_config.expected_dataset_paths,
                                                                        self.dataset_config.expected_dataset_providers)

                self.dataset_matcher.matches(expected_dataset_table_name_to_records,
                                             self.dataset_config.expected_dataset_matchers)
        finally:
            if self.dataset_config.execute_statements_after:
                self._execute_statements(self.dataset_config.execute_statements_after)

            if self.dataset_config.execute_scripts_after:
                self._execute_scripts(self.dataset_config.execute_scripts_after)

            if self.dataset_config.cleanup_after:
                self.database_executor.cleanup_tables(self.dataset_config.cleanup_tables)

    def _process_datasets(self, paths: list[str], providers: list[Callable[[], dict[str, list[dict[str, Any]]]]]):
        datasets = [d for path in paths or [] if (d := self.dataset_loader.load_dataset(str(self._resolve_path(path)))) is not None]
        datasets.extend([provider() for provider in providers or []])
        return self._merge_datasets(datasets)

    def _merge_datasets(self, datasets: list[dict[str, list[dict[str, Any]]]]) -> dict[str, list[dict[str, Any]]]:
        table_to_records = {}
        for dataset in datasets:
            for table_name, records in dataset.items():
                if table_name in table_to_records:
                    table_to_records[table_name].extend(records)
                else:
                    table_to_records[table_name] = records
        return table_to_records

    def _interpolate_variables(self, table_name_to_records: dict[str, list[dict[str, Any]]]):
        for table_name, records in table_name_to_records.items():
            for record in records:
                for column_name, column_value in record.items():
                    if type(column_value) is str:
                        record[column_name] = column_value.format(**self.dataset_config.dataset_variables)

    def _execute_scripts(self, script_paths: list[str]):
        for script_path in script_paths:
            self._execute_script(script_path)

    def _execute_script(self, script_path: str):
        for sql_stmt in self._resolve_path(script_path).read_text().split(";"):
            clean_statement = sql_stmt.strip()
            if clean_statement:
                self.database_executor.execute_query(clean_statement)

    def _execute_statements(self, statements: list[str]):
        for stmt in statements:
            self.database_executor.execute_query(stmt)

    def _resolve_path(self, relative_path: str) -> Path:
        return self.relative_path_starting_point.joinpath(relative_path)
