from logging import Logger
from os import cpu_count
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import List
from vendor.logging_utils import logger
from vendor.config.configuration import Configuration
from vendor.companies.datastore.datastore import Datastore
from vendor.companies.datastore.factory import DatastoreFactory
from vendor.logging_utils import WithLogger
from vendor.companies.models.generic_model import GenericModel
from vendor.companies.providers.data_provider import DataProvider
from vendor.companies.providers.datasource import Datasource
from vendor.companies.reflection.dymanic_import_utils import import_class
from vendor.companies.transformers.generic_transformer import GenericTransformer


class BatchExecutor:

    _batch_id: str = None
    _max_io_workers: int = 1
    _datastore_factory: DatastoreFactory
    _batch: List[Datasource]
    _transformer: GenericTransformer

    def __init__(self, id, max_io_workers: int, datastore_factory: DatastoreFactory, batch: List[Datasource], transformer: GenericTransformer):
        self._batch_id = id
        self._max_io_workers = max_io_workers
        self._datastore_factory = datastore_factory
        self._batch = batch
        self._transformer = transformer

    def execute(self):
        _process_datasource_internal(self._max_io_workers, self._datastore_factory, self._batch, self._transformer)

    def __repr__(self) -> str:
        return self._batch_id

class DataProcessor(metaclass=WithLogger):

    _task_name: str = None
    _datastore_factory: DatastoreFactory = None
    _data_provider: DataProvider = None
    _max_processes: int = 1
    _max_io_workers: int = 1
    _configuration: Configuration = None

    def __init__(self, name: str, data_provider: DataProvider, datastore_factory: Datastore, configuration: Configuration) -> None:
        self._task_name = name
        self._data_provider = data_provider
        self._datastore_factory = datastore_factory
        self._max_processes = configuration.property(
            "processor.max-processes", cpu_count())
        self._max_io_workers = configuration.property(
            "processor.max-io-workers", round(cpu_count() / 2))
        self._configuration = configuration

    def datasources(self):
        self.info(f"Initialize datasource for process {self}")

        datastore = self._datastore_factory.create()
        datastore.initialize()
        datastore.setup()
        datastore.close()

        with self._data_provider:
            self.info(
                f"Start processing total {self._data_provider.total()} datasources")
            total_slices = self._max_processes
            batch_size = round(self._data_provider.total() / total_slices)
            batch_size = 1 if batch_size <= 1 else batch_size
            batches: List[List[Datasource]] = []

            for _ in range(total_slices):
                batches.append([])

            batches.append([])

            current_batch = 0
            for datasource in self._data_provider.datasources():
                batches[current_batch].append(datasource)

                if len(batches[current_batch]) == batch_size:
                    current_batch = current_batch + 1

            transformer = self._create_transformer()

            id = 1
            for batch in batches:
                if batch:
                    yield BatchExecutor(f"datasource-batch-executor-{id}", self._configuration, self._datastore_factory, batch, transformer)
                    id = id + 1   

    def _create_transformer(self) -> GenericTransformer:
        transformer = None
        if self._configuration:
            transformer_class_name = self._configuration.property(
                "processor.transformer.transformer", "GenericTransformer")
            transformer_class = import_class(
                "vendor.companies.transformers", transformer_class_name)
            transformer = transformer_class(Configuration(
                self._configuration.property("processor.transformer.parameters", {})))
        else:
            transformer_class = import_class(
                "vendor.companies.transformers",  "GenericTransformer")
            transformer = transformer_class(Configuration({}))
        return transformer

    def __str__(self) -> str:
        return f"{self._task_name.upper()}-PROCESS"
    

def _process_datasource_internal(configuration: Configuration, datastore_factory: DatastoreFactory, datasources: List[Datasource], transformer: GenericTransformer):
    log = logger(__name__)
    max_workers = configuration.property("processor.max-io-workers", 1)
    try:
        datastore = datastore_factory.create()
        
        if max_workers > 1:
            io_datasource_executor = ThreadPoolExecutor(
                max_workers=max_workers)

            current_datasource_slice = datasources
            while current_datasource_slice:
                futures: List[Future[List[GenericModel]]] = []
                for i in range(max_workers):
                    if i < len(current_datasource_slice):
                        log.info(f"Processing datasource {current_datasource_slice[i]}")
                        futures.append(io_datasource_executor.submit(
                            _process_datasource, log, current_datasource_slice[i], datastore, transformer))
                    else:
                        break

                wait(futures)

                if max_workers > len(current_datasource_slice):
                    break
                else:
                    current_datasource_slice = current_datasource_slice[max_workers:]
        else:
            for datasource in datasources:
                _process_datasource(log, datasource, datastore, transformer)

        log.info("All batch datasources are processed")
        datastore.close()
    except Exception as ex:
        raise ex

def _process_datasource(logger: Logger, datasource: Datasource, datastore: Datastore, transformer: GenericTransformer) -> List[GenericModel]:
    try:
        logger.info(f"Processing datasource {datasource}")
        datasource.load()
        for slice in datasource.slices():
            for data in slice:
                try:
                    model = transformer.transform(data)
                    model.save()
                    if model.validate():
                        if model.filter():
                            datastore.bulk_insert(model)
                        else:
                            logger.debug(f"Data record was filtered {model}")
                    else:
                        logger.debug(f"Data model is invalid {model}")
                except Exception as ex:
                    logger.error(
                        f"Could not transform data model {data}", ex)
        datasource.release()
    except Exception as ex:
        logger.warn(f"Could not load datasource {datasource} cause {ex}")
