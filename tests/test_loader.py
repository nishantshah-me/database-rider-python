from pathlib import Path

from _pytest.outcomes import fail

from lib.loader import YamlDataSetLoader, DelegatingDataSetLoader, JsonDataSetLoader


def test_yaml_loader():
    loader = YamlDataSetLoader()
    dataset = loader.load_dataset(__abs_path("datasets/loader.yaml"))
    assert len(dataset["users"]) == 2
    assert dataset["users"][0]["id"] == 1
    assert dataset["users"][0]["name"] == "John"
    assert dataset["users"][1]["id"] == 2
    assert dataset["users"][1]["name"] == "Bob"
    assert len(dataset["statuses"]) == 1
    assert dataset["statuses"][0]["id"] == 1
    assert dataset["statuses"][0]["status"] == "Active"


def test_json_loader():
    loader = YamlDataSetLoader()
    dataset = loader.load_dataset(__abs_path("datasets/loader.json"))
    assert len(dataset["users"]) == 2
    assert dataset["users"][0]["id"] == 1
    assert dataset["users"][0]["name"] == "John"
    assert dataset["users"][1]["id"] == 2
    assert dataset["users"][1]["name"] == "Bob"
    assert len(dataset["statuses"]) == 1
    assert dataset["statuses"][0]["id"] == 1
    assert dataset["statuses"][0]["status"] == "Active"


def test_delegating_loader():
    loader = DelegatingDataSetLoader({
        "json": JsonDataSetLoader(),
        "yaml": YamlDataSetLoader(),
        "yml": YamlDataSetLoader()
    })
    dataset = loader.load_dataset(__abs_path("datasets/loader.json"))
    assert len(dataset["users"]) == 2
    assert len(dataset["statuses"]) == 1

    dataset = loader.load_dataset(__abs_path("datasets/loader.yml"))
    assert len(dataset["users"]) == 2
    assert len(dataset["statuses"]) == 1

    dataset = loader.load_dataset(__abs_path("datasets/loader.yaml"))
    assert len(dataset["users"]) == 2
    assert len(dataset["statuses"]) == 1


def test_delegating_loader_wrong_extension():
    loader = DelegatingDataSetLoader({
        "json": JsonDataSetLoader(),
        "yaml": YamlDataSetLoader(),
        "yml": YamlDataSetLoader()
    })
    try:
        loader.load_dataset(__abs_path("dataset/loader.xml"))
        fail()
    except AttributeError:
        pass


def test_delegating_loader_path_not_specified():
    loader = DelegatingDataSetLoader({
        "json": JsonDataSetLoader(),
        "yaml": YamlDataSetLoader(),
        "yml": YamlDataSetLoader()
    })
    try:
        loader.load_dataset(None)
        fail()
    except AttributeError:
        pass


def test_delegating_loader_extension_not_specified():
    loader = DelegatingDataSetLoader({
        "json": JsonDataSetLoader(),
        "yaml": YamlDataSetLoader(),
        "yml": YamlDataSetLoader()
    })
    try:
        loader.load_dataset("test")
        fail()
    except AttributeError:
        pass


def test_json_loader_path_not_specified():
    loader = JsonDataSetLoader()
    try:
        loader.load_dataset(None)
        fail()
    except AttributeError:
        pass


def test_yaml_loader_path_not_specified():
    loader = YamlDataSetLoader()
    try:
        loader.load_dataset(None)
        fail()
    except AttributeError:
        pass


def __abs_path(rel_path: str) -> str:
    return str(Path(__file__).parent.joinpath(rel_path))
