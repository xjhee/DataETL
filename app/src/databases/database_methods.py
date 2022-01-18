from app.src.databases.database_schema import database, engine, metadata
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from configparser import ConfigParser
import numpy as np
import sqlalchemy
import ormar
import databases
import asyncio
import uuid
import datetime 
import logging 


"""
:file: 
This file contains methods on Ormar - Postgres DB operations

""" 

class TableMethods(object):

    def __init__(self):
        """
        Initialize and create Ormar models
        """
        self.database = database
        self.metadata = metadata
        self.engine = engine
        self.metadata.create_all(self.engine)



    async def get_first_row(self, Table_name: object):
        """
        Get the first row from table 

        :param: Table_name: object of ormar table model

        :return: a list containing first row as list
        """
        try:
            first = await Table_name.objects.first()
            logging.info('First row: %s', first)
            return first
        except ormar.exceptions.NoMatch as e:
            logging.info('Empty table')



    async def get_all(self, Table_name: str):
        """
        Get all rows from table 
        
        :param: Table_name: object of ormar table model

        :return: a list containing all rows as list
        """
        try:
            data = await Table_name.objects.all()
            logging.info('Read table succeed')
            return data
        except ormar.exceptions.NoMatch as e:
            logging.info('Empty table')



    async def add_entity_tbl_level0(self, table0_name: object, region: str, level0_entry_details: str, exception_message_detailed: str, level1_exception_message_filtered: str, level2_exception_classes: str, level3_exception_causes: str):
        """
        Add row to table 0 
        
        :param: table0_name: object of ormar table model
        :param: region: kibana data region
        :param: level0_entry_details: level 0 exception log
        :param: exception_message_detailed: level 0 filtered exception log
        :param: level1_exception_message_filtered: level 1 exception log
        :param: level2_exception_classes: level 2 exception log
        :param: level3_exception_causes: level 3 exception log

        """        
        try:
            await table0_name.objects.create(
                                    id = uuid.uuid4(),
                                    region = region,
                                    created_at = datetime.datetime.now(),
                                    level0_entry_details = str(level0_entry_details),
                                    exception_message_detailed = exception_message_detailed,
                                    level1_exception_message_filtered = level1_exception_message_filtered,
                                    level2_exception_classes = level2_exception_classes,
                                    level3_exception_causes = level3_exception_causes)
            logging.info('Added new entity to tbl0')
        except:
            logging.info('Failed to add entity to tbl0')



    async def add_entity_tbl_level1(self, table1_name: object, region: str, level1_exception_message_filtered: str, level2_exception_classes: str, is_known: bool):
        """
        Get all rows from table1 
        
        :param: table1_name: object of ormar table model
        :param: region: kibana data region
        :param: level1_exception_message_filtered: level 1 exception log
        :param: level2_exception_classes: level 2 exception log
        :param: is_known: whether this log in marked/known by end user

        """        
        try:
            await table1_name.objects.create(
                                    id = uuid.uuid4(),
                                    region = region,
                                    level1_exception_message_filtered = level1_exception_message_filtered,
                                    level2_exception_classes = level2_exception_classes,
                                    created_at = datetime.datetime.now(),
                                    last_updated_at = datetime.datetime.now(), 
                                    num_of_occurences = 1, 
                                    is_known = is_known)
            logging.info('Added new entity to tbl1')
        except:
            logging.info('Failed to add entity to tbl1')



    async def modify_entity_tbl_level1(self, table1_name: object, level1_exception_message_filtered: str, level2_exception_classes: str, occurrences_to_add: int = None):
        """
        Modify selective rows from table1 
        
        :param: table1_name: object of ormar table model
        :param: region: kibana data region
        :param: level1_exception_message_filtered: level 1 exception log
        :param: level2_exception_classes: level 2 exception log
        :param: occurrences_to_add: if occurrences_to_add is true, we add occurrences_to_add to existing count; if not add 1

        """         
        try:
            match = await table1_name.objects.filter(level2_exception_classes = level2_exception_classes, level1_exception_message_filtered = level1_exception_message_filtered).first()
            match.last_updated_at = datetime.datetime.now()
            if occurrences_to_add:
                match.num_of_occurences = occurrences_to_add
                logging.info('Modified entity in tbl1')
            else:
                match.num_of_occurences += 1
                logging.info('Modified entity in tbl1')
            await match.update()
        except ormar.exceptions.NoMatch: 
            logging.info('Exception does not exist in tbl1')



    async def delete_entity(self, table1_name: object, level1_exception_message_filtered: str, level2_exception_classes: str):
        """
        Delete selective entity from table1 
        
        :param: table1_name: object of ormar table model
        :param: level1_exception_message_filtered: level1 exception message
        :param: level2_exception_classes: level2 exception message
        """        
        try:
            match = await table1_name.objects.filter(level1_exception_message_filtered = level1_exception_message_filtered, level2_exception_classes = level2_exception_classes).all()
            if match:
                await table1_name.objects.delete(level1_exception_message_filtered = level1_exception_message_filtered, level2_exception_classes = level2_exception_classes)
        except:
            logging.info('Failed to delete entity')



    async def update_frequent_exceptions(self, table1_name: object, known_exception_list: list = [], percentile: int = 90):
        """
        Given percentile, update list of frequent exceptions on table1 
        
        :param: table1_name: object of ormar table model
        :param: known_exception_list: list of known exception list, which are all rows from table 2
        :param: percentile: user defined percentile

        :return: a list containing updated frequent exiting exceptions
        """        
        try:
            list_num_occurrences = await table1_name.objects.values_list('num_of_occurences')
            list_num_occurrences_all = [x[0] for x in list_num_occurrences]
            shreshold = np.percentile(list_num_occurrences_all, percentile)
            updated_list_frequent_exceptions = await table1_name.objects.filter(num_of_occurences__gt = shreshold).all()

            updated_valid_list_frequent_exceptions = []
            for entry in updated_list_frequent_exceptions:
                if entry.level1_exception_message_filtered not in known_exception_list:
                    updated_valid_list_frequent_exceptions.append(entry)
            return updated_valid_list_frequent_exceptions
        except:
                logging.info('Failed to query and add tbl2')
                return []



    async def query_check_tbl1(self, table1_name: object, level1_exception_message_filtered: str, level2_exception_classes: str):
        """
        Check the combination of level1 and level2 exception exists in table1 or not
        
        :param: table1_name: object of ormar table model
        :param: level1_exception_message_filtered: level1 exception message
        :param: level2_exception_classes: level2 exception message

        :return: Boolean value; true if exists, false if not
        """         
        try:
            list_matched = await table1_name.objects.filter(level1_exception_message_filtered = level1_exception_message_filtered, level2_exception_classes = level2_exception_classes).first()
            return True
        except ormar.exceptions.NoMatch:
            return False 



    async def query_timestamps(self, table_name: object, start_time: str, end_time: str):
        """
        Given timestamps, check newly existing exceptions in table1
        
        :param: table_name: object of ormar table model
        :param: start_time: start time in created_at column
        :param: end_time: end time in created_at column

        :return: list of matched entries
        """          
        try:
            list_matched = await table_name.objects.filter(created_at__gte = str(start_time), created_at__lte = str(end_time)).all()
            return list_matched
        except ormar.exceptions.NoMatch:
            logging.info('No such time period exists')




    async def modify_table1_add_table2(self, table1_name: object, table2_name: object, 
                                        region: str,
                                        level1_exception_message_filtered: str, 
                                        level2_exception_classes: str):
        """
        Modify specific entry in table1 and add this entry to table2
        is used for end user marking known exception purposes
        
        :param: table1_name: object of ormar table1 model
        :param: table2_name: object of ormar table2 model
        :param: region: kibana region
        :param: level1_exception_message_filtered: level2 exception message
        :param: level2_exception_classes: level2 exception message
        """         
        try:
            match = await table1_name.objects.filter(level2_exception_classes = level2_exception_classes, level1_exception_message_filtered = level1_exception_message_filtered).first()
            match.is_known = True
            await match.update()
            await table2_name.objects.create(id = uuid.uuid4(),
                                    region = region,
                                    level1_exception_message_filtered = level1_exception_message_filtered,
                                    level2_exception_classes = level2_exception_classes,
                                    created_at = datetime.datetime.now())

        except:
            logging.info('Failed to modify_table1_add_table2')



    async def with_connect(self, function, *args):
        """ Connect with database """
        async with self.database:
            await function(*args)




if __name__ == '__main__':

    table_object = TableMethods()
    asyncio.run(table_object.with_connect(table_object.get_first_row, \
                                tbl_level2_frequent_exceptions))