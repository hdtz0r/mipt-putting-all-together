from sqlalchemy.orm import declarative_base
from vendor.models.data_container import DataContainer

Base = declarative_base()

class GenericModel():

    def __init__(self, data: DataContainer):
        self._raw_data = data

    def save(self):
        pass

    def validate(self) -> bool:
        return False
    
    def filter(self) -> bool:
        return True
