
from typing import Dict, List

from vendor.companies.providers.content_loader import ContentLoader


class Datasource:

    _name: str
    _values: List[Dict[str, any]] = []
    _loader: ContentLoader = None

    def __init__(self, name: str, loader: ContentLoader) -> None:
        self._name = name
        self._loader = loader

    def load(self):
        self._values = self._loader.load()

    def release(self):
        self._values = []

    def slices(self, num: int = 1):
        batch_size = 1 if num <= 1 else round(len(self._values) / num)
        current_offset = 0
        if batch_size > 1:
            for _ in range(num-1):
                yield self._values[current_offset:current_offset+batch_size]
                current_offset += batch_size

        yield self._values[current_offset:]

    def name(self) -> str:
        return self._name

    def __str__(self) -> str:
        return self._name
