
from typing import List

from vendor.companies.models.generic_model import GenericModel


class BulkInsert:

    _limit: int
    _models: List[GenericModel]

    def __init__(self, batch_size: int) -> None:
        self._limit = batch_size
        self._models = []
    
    def store(self, model: GenericModel):
        self._models.append(model)

    def models(self):
        for value in self._models:
            yield value

    def is_completed(self) -> bool:
        return len(self._models) >= self._limit