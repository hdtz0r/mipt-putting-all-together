from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()

class Vacancy(Base):
    __tablename__ = "vacancy"

    id = Column(Integer, primary_key=True)
    company = Column(String(256))
    carrier_position = Column(String(256))
    description = Column(String())
    internal_id = Column(Integer, unique=True)
    currency = Column(String(16))
    salary_from = Column(Integer)
    salary_to = Column(Integer)
    industries = Column(String())
    company_site = Column(String())

    skills = relationship("Skill", back_populates="vacancy", cascade="all, delete-orphan")

    def __repr__(self) -> str:
        return f"Vacancy(id={self.id!r}, title={self.company!r}, carrier_position={self.carrier_position!r}, internal_id={self.internal_id!r})"


class Skill(Base):
    __tablename__ = "skill"

    id = Column(Integer, primary_key=True)
    name = Column(String(512))
    vacancy_id = Column(Integer, ForeignKey("vacancy.id"))

    vacancy = relationship("Vacancy", back_populates="skills")

    def __repr__(self) -> str:
        return f"Skill(id={self.id!r}, name={self.name!r})"
    
