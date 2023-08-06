from typing import List


class DataContainer(dict):

    def get(self, key):
        value = self._get_property_internal(key)
        return value

    def get(self, key, default=None):
        value = self._get_property_internal(key)
        return value if value else default

    def _get_property_internal(self, name: str) -> any:
        parts: List[str] = name.split(".")
        value: any = self
        for part in parts:
            try:
                value = value.__getitem__(part)
            except KeyError:
                return None

            if value and isinstance(value, dict):
                continue
            else:
                break

        return value

    def __getitem__(self, __key: any) -> any:
        value = None
        try:
            value = super().__getitem__(__key)
        except KeyError:
            try:
                value = self.__getattribute__(__key)
            except AttributeError:
                pass

        return value
