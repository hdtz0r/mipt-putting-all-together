
from typing import List
from vendor.config.configuration import Configuration
from vendor.logging_utils import WithLogger
from vendor.companies.providers.datasource import Datasource


class DataProvider(metaclass=WithLogger):

    _datasources: List[Datasource]

    def __init__(self, configuration: Configuration) -> None:
        self._datasources = []

    def __enter__(self):
        self.initialize()

    def __exit__(self, type, value, traceback):
        self.cleanup()
        for datasource in self._datasources:
            try:
                datasource.release()
            except Exception as ex:
                self.warn(f"Could not release datasource {datasource} cause {ex}")

    def initialize(self):
        pass

    def total(self) -> int:
        return len(self._datasources)

    def add_datasource(self, datasource: Datasource):
        self._datasources.append(datasource)

    def datasources(self):
        for datasource in self._datasources:
            yield datasource
                
    def cleanup(self):
        pass
