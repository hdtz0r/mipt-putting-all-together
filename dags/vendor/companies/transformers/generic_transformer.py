
from typing import Dict, Type
from vendor.models.data_container import DataContainer
from vendor.config.configuration import Configuration
from vendor.logging_utils import WithLogger
from vendor.companies.models.generic_model import GenericModel
from vendor.companies.reflection.dymanic_import_utils import import_class

class GenericTransformer(metaclass=WithLogger):

    _configuration: Configuration = None
    _model_ctor: Type[GenericModel] = None

    def __init__(self, configuration: Configuration) -> None:
        self._configuration = configuration
        model_class = import_class("vendor.companies.models", configuration.property("model", "GenericModel"))
        self._model_ctor = model_class

    def transform(self, data: Dict[str, any]) -> GenericModel:
        return self._model_ctor(DataContainer(data))