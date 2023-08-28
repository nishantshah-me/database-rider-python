from dataclasses import dataclass
from typing import Callable, Any


@dataclass
class DataSetConfig(object):
    """
    List of dataset file paths. Supported formats/extensions: json, yaml/yml.

    Example: ["datasets/this_data.yaml", "datasets/that_data.json"]
    """
    dataset_paths: list[str] | None
    """
    List of dataset providers as a way of providing a dataset programmatically i.e. for generating data with Faker.
    
    Example:
    from faker import Faker
    
    def my_dataset_provider():
        fake = Faker()
        records = []
        for _ in range(10):
            records.append({'name': fake.name(), 'active': fake.pybool()})
        return {'users': records}
    """
    dataset_providers: list[Callable[[], dict[str, list[dict[str, Any]]]]] | None
    """
    Dataset Variables. A dataset values can contain variables in a python "str.format" format.
    Example yaml dataset:
    users:
     - uuid: {uuid}
       name: {person_name}
       phone: "1234567"
    
    Example variables:
    import uuid
    
    vars = {
        "person_name": "John",
        "uuid": str(uuid.uuid4())
    }
    """
    dataset_variables: dict[str, Any] | None
    """
    Specifies whether or not to cleanup tables before test
    """
    cleanup_before: bool | None
    """
    Specifies whether or not to cleanup tables after test
    """
    cleanup_after: bool | None
    """
    List of tables to cleanup before/after test execution.
    The list of tables should be ordered taking into account foreign key constraints.
    If omitted all tables will be cleaned up.
    """
    cleanup_tables: list[str] | None
    """
    List of SQL scripts to execute before test. It can contain any number of statements separated by ";"
    
    Example:
    ['sql/setup.sql']
    """
    execute_scripts_before: list[str] | None
    """
    List of SQL statements to execute before test.
    
    Example:
    ["INSERT INTO users (id, name) VALUES (1, 'John')"]
    """
    execute_statements_before: list[str] | None
    """
        List of SQL scripts to execute after test. It can contain any number of statements separated by ";"

        Example:
        ['sql/tear_down.sql']
    """
    execute_scripts_after: list[str] | None
    """
    List of SQL statements to execute after test.

    Example:
    ["DELETE FROM users WHERE id = 1"]
    """
    execute_statements_after: list[str] | None
    """
    List of expected dataset file paths. Supported formats/extensions: json, yaml/yml.
    It allows to check if records in the database match records specified in the expected dataset after test execution.

    Example: ["expected_datasets/this_data.yaml", "expected_datasets/that_data.json"]
    """
    expected_dataset_paths: list[str] | None
    """
    List of expected dataset providers as a way of providing a dataset programmatically i.e.
    to use previously generated dataset.
    
    Example:
    expected_dataset = {
        "users": [
            {
                "id": 1,
                "name": "John"
            }
        ]
    }
    
    def my_expected_dataset_provider():
        return expected_dataset
    """
    expected_dataset_providers: list[Callable[[], dict[str, list[dict[str, Any]]]]] | None
    """
    Expected dataset matchers allows user to check database value programmatically via python function.
    The first param of the function is a value that's being matched.
    The second param is a full record represented as a dict where key - column name, value - record value.
    
    Example dataset:
    users:
     - id: 1
       name: "matcher:name_matcher"
       ip: "matcher:ip_matcher"
       hash: "matcher:hash_matcher"
    Example matchers:
    import re
    from IPy import IP
    
    def check_ip(value, record):
        try:
            IP(value)
            return True
        except:
            return False
    
    expected_dataset_matchers = {
        "name_matcher": lambda value, record: value == f"John {record['id']}"
        "ip_matcher": check_ip,
        "hash_matcher": lambda value, record: re.match('[0-9a-f]{32}', value)
    }
    """
    expected_dataset_matchers: dict[str, Callable[[Any, dict[str, Any]], bool]] | None
