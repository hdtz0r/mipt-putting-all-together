from vendor.config.provider import Configuration
from vendor.logging_utils import logger
from vendor.companies.providers.data_provider import DataProvider
from vendor.companies.reflection.dymanic_import_utils import import_class

def process_company_registry(configuration: Configuration):
    log = logger(__name__)
    from vendor.companies.data_processor import DataProcessor
    from vendor.companies.datastore.factory import DatastoreFactory

    for name, process_configuration in configuration.each("processes"):
        if process_configuration.property("disabled"):
            continue

        datasource_configuration = Configuration(
            process_configuration.property("datasource"))
        datastore_configuration = Configuration(
            process_configuration.property("datastore"))

        if not datastore_configuration or not datasource_configuration:
            log.warn(
                f"The {name}'s process configuration is mess. U should configure both datastore and datasource")
        else:
            try:
                data_provider = _create_data_provider(datasource_configuration)
                connection_url = datastore_configuration.property("connection-url")
                driver = datastore_configuration.property("driver")
                parameters = Configuration(datastore_configuration.property("parameters", {}))
                datastore_factory = DatastoreFactory(name, driver, connection_url, parameters)
                processor = DataProcessor(name, data_provider, datastore_factory, Configuration(process_configuration))
                for executor in processor.datasources():
                    yield executor
            except Exception as ex:
                log.error(f"Could not initialize {name} process", ex)
                raise ex

def _create_data_provider(configuration: Configuration) -> DataProvider:
    provider_class_name = configuration.property("provider", "FileDataProvider")
    provider_class = import_class("vendor.companies.providers", provider_class_name)
    return provider_class(configuration)