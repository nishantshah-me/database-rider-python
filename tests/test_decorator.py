from unittest.mock import patch

from peewee import Database

from database_rider import setup_db_rider
from database_rider.decorator import dataset

from database_rider.handler import DataSetHandler


@patch.object(DataSetHandler, '__init__', return_value=None, wraps=DataSetHandler.__init__)
@patch.object(DataSetHandler, 'execute_before')
@patch.object(DataSetHandler, 'execute_after')
def test_decorator(execute_after, execute_before, init):
    setup_db_rider(Database(None))
    __decorated_test()
    passed_config = init.call_args[0][0]
    assert passed_config.cleanup_before is True
    assert passed_config.cleanup_after is True
    assert passed_config.dataset_paths == ["path.yaml"]
    assert len(passed_config.dataset_providers) == 1
    assert passed_config.dataset_variables == {"var": "val"}
    assert passed_config.cleanup_tables == ["users"]
    assert passed_config.execute_scripts_before == ["before.sql"]
    assert passed_config.execute_statements_before == ["INSERT INTO users (id, name) VALUES (2, 'Bob')"]
    assert passed_config.execute_scripts_after == ["after.sql"]
    assert passed_config.execute_statements_after == ["DELETE FROM users WHERE id = 2"]
    assert passed_config.expected_dataset_paths == ["expected_dataset.yaml"]
    assert len(passed_config.expected_dataset_providers) == 1
    assert "my_matcher" in passed_config.expected_dataset_matchers


@dataset(
    dataset_paths=["path.yaml"],
    dataset_providers=[lambda: {"users": [{"id": 1, "name": "John"}]}],
    dataset_variables={"var": "val"},
    cleanup_tables=["users"],
    execute_scripts_before=["before.sql"],
    execute_statements_before=["INSERT INTO users (id, name) VALUES (2, 'Bob')"],
    execute_scripts_after=["after.sql"],
    execute_statements_after=["DELETE FROM users WHERE id = 2"],
    expected_dataset_paths=["expected_dataset.yaml"],
    expected_dataset_providers=[lambda: {"users": [{"id": 2, "name": "Bob"}]}],
    expected_dataset_matchers={"my_matcher": lambda v, r: True}
)
def __decorated_test():
    pass
