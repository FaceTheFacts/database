from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import session
from connection import Session, engine
from fetch import country_fetch

Base = declarative_base()
session = Session()


class Country(Base):
    __tablename__ = "country"
    id = Column(Integer(), primary_key=True)
    entity_type = Column(String)
    label = Column(String)
    api_url = Column(String, unique=True)


def insert_country(data: list):
    data_list = []
    for datum in data:
        new_country = Country(
            id=datum["id"],
            entity_type=datum["entity_type"],
            label=datum["label"],
            api_url=datum["api_url"],
        )
        data_list.append(new_country)
    session.add_all(data_list)
    session.commit()
    session.close()


if __name__ == "__main__":
    # Migration
    Base.metadata.create_all(engine)
    insert_country(country_fetch())
