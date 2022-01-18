from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from configparser import ConfigParser
import databases
import sqlalchemy
import asyncpg
import logging

"""
This file contains code to connect to Azure PostgreSQL DB
"""


connection_string = "postgresql://adminuser@iod-postgresql-db:TJPH7%%6yuAFtrm@iod-postgresql-db.postgres.database.azure.com:5432/log_weu"
database = databases.Database(connection_string)
metadata = sqlalchemy.MetaData()
engine = create_engine(connection_string)
