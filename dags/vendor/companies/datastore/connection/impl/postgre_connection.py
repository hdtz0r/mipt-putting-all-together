from sqlalchemy.engine.base import Engine
from sqlalchemy.orm import Session
from vendor.companies.models.generic_model import GenericModel
from vendor.config.configuration import Configuration
from vendor.companies.datastore.connection.datastore_connection import DatastoreConnection
from vendor.companies.datastore.operations.bulk_insert import BulkInsert


class PostgreConnection(DatastoreConnection):

    _connection: Session = None
    _engine: Engine = None

    def __init__(self, connection_string: str, configuration: Configuration, engine: Engine):
        super().__init__(connection_string, configuration)
        self._engine = engine

    def open(self):
        self.close()
        self._connection = Session(bind=self._engine)

    def bulk_insert(self, query: BulkInsert):
        if self._connection:
            try:
                self._connection.add_all([x for x in query.models()])
                self._connection.commit()
            except:
                self._connection.rollback()
                raise

    def insert(self, model: GenericModel):
        if self._connection:
            try:
                self._connection.add(model)
                self._connection.commit()
            except:
                self._connection.rollback()
                raise

    def execute_script(self, sql: str):
        if self._connection:
            try:
                self._connection.execute(sql)
                self._connection.commit()
            except:
                self._connection.rollback()
                raise

    def execute(self, sql_query: str, **kwargs: any) -> any:
        if self._connection:
            try:
                result = self._connection.execute(sql_query, kwargs)
                self._connection.commit()
                return result
            except:
                self._connection.rollback()
                raise

    def test(self) -> bool:
        if self._connection:
            try:
                self._connection.execute("SELECT 1;")
                return True
            except Exception:
                return False

    def close(self):
        if self._connection:
            try:
                self._connection.close()
            except Exception as ex:
                self.debug(
                    f"Could not gracefully close a connection {self._connection_string}", ex)
