from typing import Generator, Tuple
from typing_extensions import Self
from vendor.models.data_container import DataContainer


class Configuration(DataContainer):

    def __init__(self, config: dict):
        super().__init__(config if config else {})

    def each(self, name: str) -> Generator[Tuple[str, Self], None, None]:
        values = self._get_property_internal(name) if name else self
        if isinstance(values, list):
            for index, value in enumerate(values):
                if isinstance(value, dict):
                    yield (str(index), Configuration(value))

        if isinstance(values, dict):
            for k in values.keys():
                value = values.get(k)
                if isinstance(value, dict):
                    yield (k, Configuration(value))

    def property(self, name: str, default: any = None) -> any:
        return self.get(name, default)
