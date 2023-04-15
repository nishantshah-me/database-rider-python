from peewee import Database
from playhouse.reflection import generate_models
from typing import Any


class DatabaseExecutor(object):
    def __init__(self, database: Database):
        self.database = database
        self.table_name_to_model_mapping = {}

    def init(self):
        self.table_name_to_model_mapping = generate_models(self.database, literal_column_names=True)

    def insert_records(self, table_name_to_records: dict[str, list[dict[str, Any]]]):
        for table_name, records in table_name_to_records.items():
            table = self.table_name_to_model_mapping[table_name]
            table.insert_many(records).execute()

    def execute_query(self, query: str):
        self.database.execute_sql(query)

    def cleanup_tables(self, tables: list[str] = None):
        if tables:
            for table in tables:
                self.table_name_to_model_mapping[table].truncate_table()
        else:
            for model in self.table_name_to_model_mapping.values():
                model.truncate_table(cascade=True)

    def fetch_all(self, table_name: str) -> list[dict[str, Any]]:
        return self.table_name_to_model_mapping[table_name].select().dicts()
