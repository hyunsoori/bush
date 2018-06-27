import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, DateTime

engine = create_engine(os.environ['MYSQL_URL'])


def lambda_handler(event, context):
    pass


if __name__ == "__main__":
    lambda_handler(None, None)

