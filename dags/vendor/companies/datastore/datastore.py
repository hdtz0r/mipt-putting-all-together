from concurrent.futures import ThreadPoolExecutor
from logging import Logger
from typing import Callable
from vendor.config.configuration import Configuration
from vendor.logging_utils import WithLogger
from vendor.companies.models.generic_model import GenericModel
from vendor.companies.reflection.dymanic_import_utils import import_class
from vendor.companies.datastore.connection.datastore_connection import DatastoreConnection
from vendor.companies.datastore.connection.datastore_connection_pool import DatastoreConnectionPool

class Datastore(metaclass=WithLogger):

    _connection_string = None
    _configuration: Configuration = None
    _connection_pool: DatastoreConnectionPool
    _init_datastore_sql: str = None
    _executor: ThreadPoolExecutor = None

    def __init__(self, name: str, connection_string: str, configuration: Configuration):
        self._connection_string = connection_string
        self._configuration = configuration
        self._init_datastore_sql = configuration.property("init-storage-sql")
        pool_size = configuration.property("max-pool-size", 1)
        self._executor = ThreadPoolExecutor(max_workers=pool_size, thread_name_prefix="DataStoreExecutor")
        factory = self._set_connection_pool_factory()
        if factory:
            self._connection_pool = DatastoreConnectionPool(
                name, connection_string, factory, Configuration(configuration.property("connection", {})), pool_size)
        else:
            raise RuntimeError(
                "Connection pool is not initialized. Did ur sub class overrides _set_connection_pool_factory method to define desired connection factory")

    def initialize(self):
        self._connection_pool.init_pool()

    def _set_connection_pool_factory(self) -> Callable[[any], DatastoreConnection]:
        return None

    def bulk_insert(self, model: GenericModel):
        pass

    def insert(self, model: GenericModel):
        pass

    def migrate(self):
        pass

    def setup(self):
        if self._init_datastore_sql:
            try:
                self._accuire_and_perfome(lambda conn: Datastore._init_storage(
                    self.getLogger(), self._init_datastore_sql, conn))
            except Exception as ex:
                self.error(f"Could not execut init sql script on {self._connection_string} cause {str(ex)}")
        self.info(f"Datastore {self._connection_string} is ready")

    def _accuire_and_perfome(self, runnable: Callable[[DatastoreConnection], any]):
        return self._executor.submit(Datastore._schedule_datastore_task, self.getLogger(), self._connection_pool, runnable) 

    @staticmethod
    def _schedule_datastore_task(logger: Logger, pool: DatastoreConnectionPool, runnable: Callable[[DatastoreConnection], any]):
        connection = pool.accuire()
        try:
            return runnable(connection)
        except Exception as ex:
            logger.error("Could not perfome a task on the datastore", ex)
        finally:
            pool.release(connection)

    @staticmethod
    def _init_storage(logger: Logger, sql: str, connection: DatastoreConnection):
        try:
            connection.execute_script(sql)
        except Exception as ex:
            logger.error("Could not setup datastore", ex)

    def close(self):
        self._executor.shutdown()
        self._connection_pool.close_all()


def create_datastore(name: str, driver: str, connection_string: str, configuration: Configuration) -> Datastore:
    class_ctor = import_class("vendor.companies.datastore.impl", driver)
    return class_ctor(name, connection_string, configuration)

def load_models():
    import_class("vendor.companies.models", "Company")