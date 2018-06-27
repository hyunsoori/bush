import datetime
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class AptTradeInfo(Base):
    __tablename__ = 'apt_trade_info'

    id = Column(Integer, primary_key=True)
    key = Column(String, nullable=False)
    yyyymm = Column(String, nullable=False)
    days = Column(String, nullable=False)
    city_code = Column(String, nullable=False)
    city_name = Column(String, nullable=False)
    dong_name = Column(String, nullable=False)
    apt_name = Column(String, nullable=False)
    floor = Column(Integer, nullable=False)
    space = Column(String, nullable=True)
    price = Column(Integer, nullable=False)
    start_year = Column(String, nullable=True)
    created_date = Column(DateTime, default=datetime.datetime.now)
    updated_date = Column(DateTime, nullable=True)
