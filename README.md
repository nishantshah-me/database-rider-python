# Database Rider

Database Rider simplifies a database integration testing by providing an easy and standardised way to prepare your database for a test execution.

It's heavily inspired by the original [Database Rider for Java](https://github.com/database-rider/database-rider)

Tastes best with [TestContainers](https://github.com/testcontainers/testcontainers-python)

## Intallation
```shell
pip install database_rider
```

## Setup

Convenience `setup_db_rider` function is provided to simplify the setup process. It takes a peewee Database instance as a param.

### Examples:

#### PostgreSQL

```python
from database_rider import setup_db_rider
from peewee import PostgresqlDatabase

setup_db_rider(PostgresqlDatabase("postgresql://user:secret@localhost:5432/mydb?connect_timeout=10"))
```

#### Sqlite

```python
from database_rider import setup_db_rider
from peewee import SqliteDatabase

setup_db_rider(SqliteDatabase("test_matcher.db"))
```

If you're using a different database please check the corresponding section in
the [peewee documentation](https://docs.peewee-orm.com/en/latest/peewee/database.html).

## Usage

Using the library is as simple as applying the `@dataset` decorator to a test function and providing corresponding
datasets.

### Let's see some usage examples

#### Applying simple datasets to your database before a test execution

#### **`datasets/users.yaml`**

```yaml
users:
  - id: 1
    name: 'John'
  - id: 2
    name: 'Bob'
statuses:
  - id: 1
    status: '{status_name}'
    user_id: 1
```

#### **`datasets/roles.json`**

```json
{
  "roles": [
    {
      "id": 1,
      "role_name": "MY_ROLE"
    }
  ]
}
```

```python
from database_rider.decorator import dataset


@dataset(dataset_paths=["datasets/users.yaml", "datasets/roles.json"],
         dataset_variables={"status_name": "Active"})
def test_something():
    pass
```

This will insert 2 users, status and a role to corresponding tables. Please note that a status name is provided as a
variable `{status_name}` and interpolated from `dataset_variables` param.

#### Alternatively a dataset can be provided programmatically

```python
from database_rider.decorator import dataset
from faker import Faker


def my_dataset_provider():
    fake = Faker()
    records = []
    for _ in range(10):
        records.append({'name': fake.name(), 'active': fake.pybool()})
    return {'users': records}


@dataset(dataset_providers=[my_dataset_provider])
def test_something():
    pass
```

#### SQL scripts or statements can be executed before or after a test

#### **`sql/before.sql`**

```sql
INSERT INTO users (id, name)
VALUES (1, 'John');
INSERT INTO statuses (id, name, user_id)
VALUES (1, 'Active', 1)
```

#### **`sql/after.sql`**

```sql
DELETE
FROM users
WHERE id = 1;
DELETE
FROM statuses
WHERE id = 1;
```

```python
from database_rider.decorator import dataset


@dataset(execute_scripts_before=["sql/before.sql"],
         execute_statements_before=["UPDATE roles SET active = TRUE"],
         execute_statements_after=["UPDATE roles SET active = FALSE"],
         execute_scripts_after=["sql/after.sql"])
def test_something():
    pass
```

The `execute_scripts_before` and `execute_statements_before` will be applied before the test execution.
The `execute_scripts_after` and `execute_statements_after` will be applied after the test execution.

#### It's also possible to validate database state after the test execution

#### **`expected_datasets/users.yaml`**

```yaml
users:
  - id: 1
    name: 'John'
    ip: 'matcher: ip_matcher'
statuses:
  - id: 1
    status: 'Active'
    user_id: 1
```

#### **`expected_datasets/roles.json`**

```json
{
  "roles": [
    {
      "id": 1,
      "role_name": "matcher: role_matcher"
    }
  ]
}
```

```python
from database_rider.decorator import dataset
from IPy import IP


def check_ip(value, record):
    try:
        IP(value)
        return True
    except:
        return False


@dataset(expected_dataset_paths=["expected_datasets/roles.json", "expected_datasets/users.yml"],
         expected_dataset_matchers={
             "ip_matcher": check_ip,
             "role_matcher": lambda value, record: value == "MY_ROLE" if record["id"] == 1 else value == "OTHER_ROLE"
         })
def test_something(session):
    session.execute("INSERT INTO users (id, ip, name) VALUES (1, '127.0.0.1', 'John')")
    session.execute("INSERT INTO statuses (id, status, user_id) VALUES (1, 'Active', 1)")
    session.execute("INSERT INTO roles (id, role_name) VALUES (1, 'MY_ROLE')")
```

#### Alternatively expected dataset can be provided programmatically

```python
from database_rider.decorator import dataset

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


@dataset(expected_dataset_providers=[my_expected_dataset_provider])
def test_something(session):
    session.execute("INSERT INTO users (id, name) VALUES (1, 'John')")
```

#### Options to clean up tables before/after test execution

By default, all tables in the specified database are cleaned up before and after test execution. It's possible to alter
this behavior with `cleanup_before`, `cleanup_after` and `cleanup_tables` params.

```python
from database_rider.decorator import dataset


@dataset(cleanup_before=True, cleanup_after=False, cleanup_tables=['statuses', 'users'])
def test_something():
    pass
```

If `cleanup_tables` is set it will be executed in the order specified. It's developer's responsibility to make
sure the order takes into account foreign key constraints.

## Supported options (all optional)

| Parameter name               | Description                                                                                                                                                                                                                                                                 |
|------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `dataset_paths`              | List of dataset file paths. Supported formats/extensions: json, yaml/yml.                                                                                                                                                                                                   |
| `dataset_providers`          | List of dataset providers as a way of providing a dataset programmatically i.e. for generating data with Faker.                                                                                                                                                             |
| `dataset_variables`          | Dataset variables. A dataset values can contain variables in a python "str.format" format. Interpolates both `dataset_paths` and `dataset_providers` data.                                                                                                                  |
| `cleanup_tables`             | List of tables to clean up before/after test execution. The list of tables should be ordered taking into account foreign key constraints. If omitted all tables will be cleaned up.                                                                                         |
| `cleanup_before`             | Specifies whether to clean up tables before test. Cleans up all tables if `cleanup_tables` is omitted.                                                                                                                                                                      |
| `cleanup_after`              | Specifies whether to clean up tables after test. Cleans up all tables if `cleanup_tables` is omitted.                                                                                                                                                                       |
| `execute_scripts_before`     | List of SQL scripts to execute before test. It can contain any number of statements separated by ";"                                                                                                                                                                        |
| `execute_statements_before`  | List of SQL statements to execute before test.                                                                                                                                                                                                                              |
| `execute_scripts_after`      | List of SQL scripts to execute after test. It can contain any number of statements separated by ";"                                                                                                                                                                         |
| `execute_statements_after`   | List of SQL statements to execute after test.                                                                                                                                                                                                                               |
| `execute_statements_after`   | List of expected dataset file paths. Supported formats/extensions: json, yaml/yml.                                                                                                                                                                                          |
| `expected_dataset_providers` | List of expected dataset providers as a way of providing a dataset programmatically i.e. to use previously generated dataset.                                                                                                                                               |
| `expected_dataset_matchers`  | Expected dataset matchers allows user to check database value programmatically via python function. The first param of the function is a value that's being matched. The second param is a full record represented as a dict where key - column name, value - record value. |
