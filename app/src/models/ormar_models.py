import ormar
import asyncio
import pydantic
import databases
import sqlalchemy
import uuid
import datetime 
import logging 
from typing import List, Optional
from pydantic import BaseModel
from app.src.databases.database_schema import database, engine, metadata

"""
:file: 
This file contains Ormar models that are useful for database connection/operations
Three Ormar models corresponds to three tables in our database

""" 

logging.basicConfig(level = logging.INFO)


class BaseMeta(ormar.ModelMeta):

    database = database
    metadata = metadata


class tbl_level0_alert_details(ormar.Model):

    class Meta(BaseMeta):
        tablename = "tbl_level0_alert_details"
    id: uuid.UUID = ormar.UUID(primary_key=True, uuid_format='string')
    region: str = ormar.String(max_length=100)
    created_at: datetime.datetime = ormar.DateTime()
    level0_entry_details: str = ormar.String(max_length=30000)
    exception_message_detailed: str = ormar.String(max_length=3000)
    level1_exception_message_filtered: str = ormar.String(max_length=250)
    level2_exception_classes: str = ormar.String(max_length=250)
    level3_exception_causes: str = ormar.String(max_length=250)


class tbl_level1_exception_message_filtered(ormar.Model):

    class Meta(BaseMeta):
        tablename = "tbl_level1_exception_message_filtered"
    id: uuid.UUID = ormar.UUID(primary_key=True, uuid_format='string')
    region: str = ormar.String(max_length=100)
    level1_exception_message_filtered: str = ormar.String(max_length=250)
    level2_exception_classes: str = ormar.String(max_length=250)
    created_at: datetime.datetime = ormar.DateTime() 
    last_updated_at: datetime.datetime = ormar.DateTime(default=datetime.datetime.now()) 
    num_of_occurences: int = ormar.Integer()
    is_known: bool = ormar.Boolean()


class tbl_level2_frequent_exceptions(ormar.Model):
    
    class Meta(BaseMeta):
        tablename = "tbl_level2_frequent_exceptions"
    id: uuid.UUID = ormar.UUID(primary_key=True, uuid_format='string')
    region: str = ormar.String(max_length=100)
    level1_exception_message_filtered: str = ormar.String(max_length=250)
    level2_exception_classes: str = ormar.String(max_length=250)
    created_at: datetime.datetime = ormar.DateTime() 







if __name__ == '__main__':
    pass