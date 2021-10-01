import sys

sys.path.append("src")
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import session
from connection_test import Session, engine

Base = declarative_base()
session = Session()

class CountryTest(Base):
  __tablename__ = 'country_test'
  id = Column(Integer(), primary_key=True)
  entity_type = Column(String)
  label = Column(String)
  api_url = Column(String, unique=True)


# Migration
# Base.metadata.create_all(engine)

# country_one = CountryTest(
#   id =123,
#   entity_type = 'Country',
#   label = 'Germany',
#   api_url ="api/123/Germany"
# )

# session.add(country_one)
# session.commit()
# session.close()

#Drop Table
CountryTest.__table__.drop(engine)
session.commit()
session.close()






  


