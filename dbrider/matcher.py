from typing import Any, Callable
from .database import DatabaseExecutor


class DataSetMatcher(object):
    def __init__(self, database_executor: DatabaseExecutor):
        self.database_executor = database_executor

    def matches(self, dataset_table_name_to_records: dict[str, list[dict[str, Any]]],
                dataset_matchers: dict[str, Callable[[Any, dict[str, Any]], bool]] | None):
        for table_name, dataset_records in dataset_table_name_to_records.items():
            db_records = self.database_executor.fetch_all(table_name)
            if len(dataset_records) != len(db_records):
                raise ValueError(f"Dataset for table {table_name} contains {len(dataset_records)}"
                                 f" but found {len(db_records)} in the table")
            for db_record in db_records:
                if not self._match_record(db_record, dataset_records, dataset_matchers):
                    raise ValueError(f"Database record {db_record} doesn't match any record in the dataset")

    def _match_record(self, db_record: dict[str, Any],
                      dataset_records: list[dict[str, Any]],
                      dataset_matchers: dict[str, Callable[[Any, dict[str, Any]], bool]] | None) -> bool:
        matches = False
        for dataset_record in dataset_records:
            if matches:
                break
            matches = True
            for dataset_column, dataset_value in dataset_record.items():
                if type(dataset_value) is str and dataset_value.strip().startswith("matcher:"):
                    matcher = dataset_matchers[dataset_value.replace("matcher:", "").strip()]
                    if not matcher(db_record.get(dataset_column), db_record):
                        matches = False
                        break
                else:
                    if not db_record.get(dataset_column) == dataset_value:
                        matches = False
                        break
        return matches
