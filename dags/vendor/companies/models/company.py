from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from vendor.companies.models.generic_model import Base
from vendor.companies.models.generic_model import GenericModel


class Company(GenericModel, Base):

    __tablename__ = "company"

    id = Column(Integer, primary_key=True)
    ogrn = Column(String(32))
    inn = Column(String(32))
    kpp = Column(String(32))
    fullname = Column(String(1024))
    name = Column(String(512))
    okvd_code = Column(String(1024))

    def get(self, property: str):
        return self._raw_data.get(property)

    def get(self, property: str, default_value: any = None):
        return self._raw_data.get(property, default_value)

    def validate(self) -> bool:
        if self.get("ogrn") and self.get("data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД") and (self.get("full_name") or self.get("name")):
            return True
        return super().validate()

    def filter(self) -> bool:
        okvd_code = self.get(
            "data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД")
        if okvd_code and str(okvd_code).startswith("61"):
            return True
        return False

    def save(self):
        self.inn = self.get("inn")
        self.kpp = self.get("kpp")
        self.ogrn = self.get("ogrn")
        self.fullname = self.get("full_name", self.get("name"))
        self.name = self.get("name")
        self.okvd_code = self.get("data.СвОКВЭД.СвОКВЭДОсн.КодОКВЭД")

    def __repr__(self) -> str:
        return f"Company(id={self.id!r}, ogrn={self.ogrn!r}, inn={self.inn!r}, kpp={self.kpp!r}, name={self.name!r}, fullname={self.fullname!r}, okvd_code={self.okvd_code!r})"
