import json
from abc import ABC, abstractmethod
from typing import Any

import yaml


class DataSetLoader(ABC):
    @abstractmethod
    def load_dataset(self, path: str) -> dict[str, list[dict[str, Any]]]:
        pass


class JsonDataSetLoader(DataSetLoader):
    """
    Note: The separate Json loader is used for performance reasons.
    Yaml parser is generally significantly slower.
    """
    def load_dataset(self, path: str) -> dict[str, list[dict[str, Any]]]:
        if not path:
            raise AttributeError("Path should not be None")

        with open(path, 'r') as f:
            return json.load(f)


class YamlDataSetLoader(DataSetLoader):
    def load_dataset(self, path: str) -> dict[str, list[dict[str, Any]]]:
        if not path:
            raise AttributeError("Path should not be None")

        with open(path, 'r') as f:
            return yaml.load(f, Loader=yaml.FullLoader)


class DelegatingDataSetLoader(DataSetLoader):
    def __init__(self, loaders: dict[str, DataSetLoader]):
        self.loaders = loaders

    def load_dataset(self, path: str) -> dict[str, list[dict[str, Any]]]:
        if not path:
            raise AttributeError("Path should not be None")

        split = path.split(".")
        if len(split) < 2:
            raise AttributeError("Path should have a valid extension")

        path_extension = split[-1]
        if path_extension not in self.loaders:
            raise AttributeError(f"Path extension {path_extension} is not valid. Valid extensions: {self.loaders.keys()}")

        return self.loaders[path_extension].load_dataset(path)
