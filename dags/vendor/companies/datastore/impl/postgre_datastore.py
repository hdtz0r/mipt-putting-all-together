
from typing import Callable

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy_utils import database_exists, create_database
from vendor.config.configuration import Configuration
from vendor.companies.models.generic_model import Base
from vendor.companies.models.generic_model import GenericModel
from vendor.companies.datastore.datastore import load_models
from vendor.companies.datastore.connection.datastore_connection import DatastoreConnection
from vendor.companies.datastore.connection.impl.postgre_connection import PostgreConnection
from vendor.companies.datastore.datastore import Datastore
from vendor.companies.datastore.operations.bulk_insert import BulkInsert

class PostgreDatastore(Datastore):

    _insert_batch_size: int = None
    _bulk_insert: BulkInsert = None
    _engine: Engine = None
    _should_drop: bool = False

    def __init__(self, name: str, connection_string: str, configuration: Configuration):
        super().__init__(name, connection_string, configuration)
        self._insert_batch_size = configuration.property("batch.max-size", 5000)
        self._bulk_insert = None
        self._engine = create_engine(connection_string)
        self._should_drop = configuration.property("should-drop", False)

    def _set_connection_pool_factory(self) -> Callable[[any], DatastoreConnection]:
        return lambda connection_string, configuration: PostgreConnection(connection_string, configuration, self._engine)

    def bulk_insert(self, model: GenericModel):
        if not self._bulk_insert:
            self._bulk_insert = BulkInsert(self._insert_batch_size)

        if self._bulk_insert.is_completed():
            self._bulk_insert = None
            self._accuire_and_perfome(lambda conn: conn.bulk_insert(self._bulk_insert))
            self.bulk_insert(model)
        else:
            self._bulk_insert.store(model)

    def insert(self, model: GenericModel):
        return self._accuire_and_perfome(lambda conn: conn.insert(model))
    
    def setup(self):
        if not database_exists(self._engine.url):
            try:
                create_database(self._engine.url)
            except:
                self.error(f"Could not create database {self._engine.url}")
                raise
        try:
            if self._should_drop:
                for table in reversed(Base.metadata.sorted_tables):
                    table.drop(self._engine)
        except:
            pass
        
        load_models()
        Base.metadata.create_all(self._engine)
        super().setup()

    def close(self):
        if self._bulk_insert:
            try:
                self._accuire_and_perfome(lambda conn: conn.bulk_insert(self._bulk_insert))
                self._bulk_insert = None
            except Exception as ex:
                self.warn(f"Could not perfome batch insert cause {ex}")
        return super().close()
