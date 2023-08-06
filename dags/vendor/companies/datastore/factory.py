

from vendor.config.configuration import Configuration
from vendor.companies.datastore.datastore import Datastore, create_datastore


class DatastoreFactory:

    _name: str = None
    _driver: str = None
    _connection_url: str = None
    _parameters: Configuration = None

    def __init__(self, name: str, driver: str, connection_url: str, parameters: Configuration) -> None:
        self._name = name
        self._driver = driver
        self._connection_url = connection_url
        self._parameters = parameters

    def create(self) -> Datastore:
        return create_datastore(self._name, self._driver, self._connection_url, self._parameters)
