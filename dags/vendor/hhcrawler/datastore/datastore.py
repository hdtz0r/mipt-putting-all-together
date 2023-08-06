from typing import List
from sqlalchemy import create_engine
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm import Session
from vendor.config.configuration import Configuration
from vendor.hhcrawler.models.vacancy import Base

engine = None

def initialize(configuration: Configuration):
    global engine
    engine = create_engine(configuration.property('datastore.connection-string'))
    if not database_exists(engine.url):
        create_database(engine.url)

    try:
        for table in reversed(Base.metadata.sorted_tables):
            table.drop(engine)
    except:
        pass
    Base.metadata.create_all(engine)


def save_all(models: List[Base]):
    with Session(bind=engine) as session:
        try:
            session.add_all(models)
            session.commit()
        except Exception as ex:
            session.rollback()
            raise ex
