
from queue import Queue
from typing import Callable, Type
from vendor.logging_utils import WithLogger
from vendor.config.configuration import Configuration
from vendor.companies.datastore.connection.datastore_connection import DatastoreConnection
from vendor.companies.datastore.utils import retry

class DatastoreConnectionPool(metaclass=WithLogger):

    _pool: Queue = None

    _datastore_name: str = None
    _connection_string: str = None
    _connection_factory: Callable[[any], DatastoreConnection] = None
    _max_pool_size: int = None
    _configuration: Configuration = None

    def __init__(self, name: str, connection_string: str, factory: Callable[[any], DatastoreConnection], connection_configuration: Configuration, pool_size: int = 1) -> None:
        self._datastore_name = name
        self._configuration = connection_configuration
        self._connection_string = connection_string
        self._connection_factory = factory
        self._max_pool_size = pool_size
        self._pool = Queue(pool_size)

    def init_pool(self):
        self.info(
            f"Initializing datastore {self._datastore_name} using pool size {self._max_pool_size}")
        for _ in range(self._max_pool_size):
            try:
                self._pool.put_nowait(self._spawn_new())
            except Exception as ex:
                self.error("Could not spawn connection", ex)
                break

    @retry(Exception)
    def accuire(self) -> DatastoreConnection:
        if self._pool.empty():
            return self._spawn_new()
        
        connection = self._pool.get()
        if connection.test():
            return connection
        else:
            try:
                connection.close()
            except:
                pass
            return self._spawn_new()

    @retry(Exception)
    def release(self, connection: DatastoreConnection):
        if connection.test():
            self._pool.put_nowait(connection)
        else:
            try:
                connection.close()
            except:
                pass
            self._pool.put_nowait(self._spawn_new())

    def _spawn_new(self) -> DatastoreConnection:
        self.debug(
            f"Spawning new connection for the datasource {self._datastore_name}")
        connection = self._connection_factory(
            self._connection_string, self._configuration)
        connection.open()
        return connection

    def close_all(self):
        self.info(f"Shutdown pool for datastore {self._datastore_name}")
        while not self._pool.empty():
            try:
                conn = self._pool.get(timeout=1)
                conn.close()
                self.debug("Connection is closed")
            except:
                pass
        self.info(f"Datastores {self._datastore_name} pool is terminated")
