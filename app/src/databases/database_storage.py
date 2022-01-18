from app.src.models.ormar_models import tbl_level0_alert_details, tbl_level1_exception_message_filtered, tbl_level2_frequent_exceptions, BaseMeta
from app.src.databases.database_methods import TableMethods
from app.src.databases.data_cleansing import DataCleansing
from app.src.databases.singleton import Singleton
from configparser import ConfigParser
from typing import List
import asyncio
import datetime
import logging 
import ormar

"""
:file: 
This file contains logic to create and update database using database methods

""" 


logging.basicConfig()


class Level1ExceptionObject:

    def __init__(self, region: str, level1_exception_message_filtered: str, level2_exception_classes: str, 
                    created_at: str = None, last_updated_at: str = None, num_of_occurences: int = 0, is_known: bool = False):
        """ Level1ExceptionObject initialize a level 1 exception formatted object,
        is used for memory storage and update purpose

        :param region: Region this log comes from 
        :param level1_exception_message_filtered: Level 1 exception contents
        :param level2_exception_classes: Level 2 exception contents
        :param created_at: Datetime this log in stored into database
        :param last_updated_at: The last datetime this log is stored into database 
        :param num_of_occurences: Count of occurrences
        :param is_known: Whether this log is marked/known by end user 

        """
        self.region = region
        self.level1_exception_message_filtered = level1_exception_message_filtered
        self.level2_exception_classes = level2_exception_classes
        self.created_at = created_at
        self.last_updated_at = last_updated_at
        self.num_of_occurences = num_of_occurences
        self.is_known = is_known


class UpdateExceptionObject(Singleton):

    table_object, object_list, current_datetime = None, None, None

    def first_run(self, table_object: TableMethods = TableMethods(), 
                        object_list: List[Level1ExceptionObject] = list(), 
                        list_of_frequent_exceptions = [], 
                        current_datetime = datetime.datetime.now()):    
        """ Called when application starts, initialize attributes based on user input percentile and days

        :param table_object: Ormar table object
        :param object_list: List of Level1ExceptionObject objects
        :param list_of_frequent_exceptions: Frequent occurring exception list, is global
        :param current_datetime: Current datetime, for database storage input
    
        """              
        config = ConfigParser()
        config.read('app/config.ini')
        self.exception_percentile = int(config['outage-parameters']['percentile'])
        self.exception_days = int(config['outage-parameters']['days'])

        self.table_object = table_object
        self.object_list = object_list
        self.list_of_known_frequent_exceptions = get_known_exceptions_from_table2()
        self.list_of_frequent_exceptions = get_frequent_exceptions_from_table1(percentile = self.exception_percentile)
        self.current_datetime = current_datetime



    async def update_table0(self, data_entry: list, region: str):

        """ Store data into table 0
        :param data_entry: Data to be stored into database
        :param region: Kibana region 
        """
        asyncio.run(self.table_object.add_entity_tbl_level0(
                                        table0_name = tbl_level0_alert_details, 
                                        region = region, 
                                        level0_entry_details = data_entry[0], 
                                        exception_message_detailed = data_entry[1], 
                                        level1_exception_message_filtered = data_entry[2], 
                                        level2_exception_classes = data_entry[3], 
                                        level3_exception_causes = data_entry[4]))



    async def update_table1(self, data_entry: list, region: str):

        """ Update table1 given logic
        :param data_entry: Data to be stored into database
        :param region: Kibana region 
        """

        # If frequently occurring, skip this data point
        if data_entry[2] in self.list_of_known_frequent_exceptions:
            return
        else:
            parsed_alert = {"level1_exception_message_filtered": data_entry[2], 
                            "level2_exception_classes": data_entry[3]}
            parsed_alert_created_at, parsed_alert_last_updated_at = self.current_datetime, self.current_datetime
            converted_to_exception_object = Level1ExceptionObject(region = region, 
                                                                level1_exception_message_filtered = parsed_alert['level1_exception_message_filtered'], 
                                                                level2_exception_classes = parsed_alert['level2_exception_classes'], 
                                                                created_at = parsed_alert_created_at, 
                                                                last_updated_at = parsed_alert_last_updated_at, 
                                                                num_of_occurences = 1, 
                                                                is_known = False)
            # Update in memory
            for each in self.list_of_frequent_exceptions:
                if parsed_alert and each:
                    if each.level1_exception_message_filtered == converted_to_exception_object.level1_exception_message_filtered \
                            and each.level2_exception_classes == converted_to_exception_object.level2_exception_classes:
                            each.last_updated_at = converted_to_exception_object.last_updated_at
                            each.num_of_occurences += 1
                            logging.info('Add in memory, now num_of_occurences: %s', each.num_of_occurences)
                            return  

            # Update table1 
            check_object_table1 = asyncio.run(self.table_object.query_check_tbl1(
                                            table1_name = tbl_level1_exception_message_filtered, 
                                            level1_exception_message_filtered = parsed_alert['level1_exception_message_filtered'], 
                                            level2_exception_classes = parsed_alert['level2_exception_classes']))
            if not check_object_table1:
                asyncio.run(self.table_object.add_entity_tbl_level1(
                            table1_name = tbl_level1_exception_message_filtered, 
                            region = region, 
                            level1_exception_message_filtered = parsed_alert['level1_exception_message_filtered'], 
                            level2_exception_classes = parsed_alert['level2_exception_classes'], 
                            is_known = False))
            else:
                asyncio.run(self.table_object.modify_entity_tbl_level1(
                            table1_name = tbl_level1_exception_message_filtered, 
                            level1_exception_message_filtered = parsed_alert['level1_exception_message_filtered'], 
                            level2_exception_classes = parsed_alert['level2_exception_classes']))



    async def update_tables_during_day(self, cleaned_data: dict, region: str = 'weu'):

        """ Update two tables according to logic
        :param cleaned_data: Data to be stored into database
        :param region: Kibana region 
        """
        list_level0_entry_details, list_exception_message_detailed, \
            list_level1_exception_message_filtered, list_level2_exception_classes, \
            list_level3_exception_causes = cleaned_data['level0_entry_details'], \
                                        cleaned_data['exception_message_detailed'], \
                                        cleaned_data['level1_exception_message_filtered'], \
                                        cleaned_data['level2_exception_classes'], \
                                        cleaned_data['level3_exception_causes']
        tasks = []
        for data_entry in zip(list_level0_entry_details, list_exception_message_detailed, list_level1_exception_message_filtered, list_level2_exception_classes, list_level3_exception_causes):
            tasks.append(asyncio.ensure_future(self.update_table0(data_entry, region)))
            tasks.append(asyncio.ensure_future(self.update_table1(data_entry, region)))
        await asyncio.gather(*tasks, return_exceptions = True)



    def update_memory_job(self):

        """ Run in interval of 15 min (user can adjust the timeframe from time triggered Azure Functions)
        Logic: for every object in list_of_frequent_exceptions, update table1 counts accordingly

        """
        try:
            if self.list_of_frequent_exceptions:
                for each in self.list_of_frequent_exceptions:
                    asyncio.run(self.table_object.modify_entity_tbl_level1(
                                            table1_name = tbl_level1_exception_message_filtered, 
                                            level1_exception_message_filtered = each.level1_exception_message_filtered, 
                                            level2_exception_classes = each.level2_exception_classes, 
                                            occurrences_to_add = each.num_of_occurences))
        except NameError:
            logging.info('Day end job failed. list_of_frequent_exceptions is not defined. Try first_run function')


    
    def day_end_job(self):
        """  Run when day ends to update list_of_frequent_exceptions 
            Run in 30 minute interval (user can adjust the timeframe from time triggered Azure Functions)
        """
        try:
            frequent_exceptions = asyncio.run(
                                self.table_object.update_frequent_exceptions(
                                table1_name = tbl_level1_exception_message_filtered, 
                                known_exception_list = self.list_of_known_frequent_exceptions, 
                                percentile = self.exception_percentile))
            self.list_of_frequent_exceptions = frequent_exceptions
            self.list_of_known_frequent_exceptions = get_known_exceptions_from_table2()
        except NameError:
            logging.info('Day end job failed.')

    

    def get_new_exceptions(self):

        """  Find newly existing exceptions during the past hours/days given user input timeframe """
        start_time = (datetime.datetime.now() - datetime.timedelta(int(self.exception_days))).strftime('%Y-%m-%d %H:%M:%S')
        end_time = datetime.datetime.now()
        new_exceptions = asyncio.run(self.table_object.query_timestamps(
                                            table_name = tbl_level1_exception_message_filtered, 
                                            start_time = start_time, 
                                            end_time = end_time))
        return new_exceptions



    def update_frequent_exceptions_from_table1(self):

        """
            Update entries from table1 that are frequently occurring, 
            based on percentile threshold input by end user
        :return: a global list of object containing frequently occurring
        """       

        object_list = []
        table1_entries = asyncio.run(self.table_object.update_frequent_exceptions(
                                                table1_name = tbl_level1_exception_message_filtered, 
                                                known_exception_list = self.list_of_known_frequent_exceptions))
        if table1_entries:
            for entry in table1_entries:
                new_exception_object = Level1ExceptionObject(entry.region, entry.level1_exception_message_filtered, entry.level2_exception_classes, entry.created_at, entry.last_updated_at, entry.num_of_occurences, entry.is_known)
                object_list.append(new_exception_object)
        return object_list  

        


def get_frequent_exceptions_from_table1(percentile):
    
    """
        Get frequently existing exceptions from table1 given self defined percentile 
    :return: a list containing frequent existing exceptions
    """
    object_list = []
    table_object = TableMethods()
    table1_entries = asyncio.run(table_object.update_frequent_exceptions(
                                    table1_name = tbl_level1_exception_message_filtered, 
                                    percentile = percentile))
    if table1_entries:
        for entry in table1_entries:
            new_exception_object = Level1ExceptionObject(entry.region, entry.level1_exception_message_filtered, entry.level2_exception_classes, entry.created_at, entry.last_updated_at, entry.num_of_occurences, entry.is_known)
            object_list.append(new_exception_object)
    return object_list
 



def get_known_exceptions_from_table2():

    """
        Get all entries from table3, where stores frequently occurring known entries 
    :return: a list containing every entry in table3
    """
    known_exception_list = []
    table_object = TableMethods()
    table2_entries = asyncio.run(table_object.get_all(Table_name = tbl_level2_frequent_exceptions))
    for entry in table2_entries:
        known_exception_list.append(entry.level1_exception_message_filtered)
    return known_exception_list   



if __name__ == '__main__':
    pass
