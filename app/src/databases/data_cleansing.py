
import pandas as pd
import numpy as np
import json
import ast
import os
import glob
import re
import ast
import logging
import os.path
import asyncio
import sys
import datetime
from configparser import ConfigParser

"""
:file: 
This file contains methods to cleanse data log

""" 

logging.basicConfig(level = logging.INFO)

class DataCleansing(object):

    def __init__(self, json_data: dict):
        
        self.json_data = json_data



    def json_to_dataframe(self):
        """
        Convet json to dataframe

        :return: a dataframe of raw data
        """ 
        df_all = pd.DataFrame()
        df_all = pd.DataFrame(self.json_data, index = [0])
        return df_all



    def format_log(self, log: str):
        """
        Clean log
        
        :param: log: raw log data from kibana

        :return: string of formatted log
        """ 
        try:
            total_log = json.loads(log.replace('&quot;', '"'))
            return total_log
        except:
            try:
                log = re.sub(r'^{&quot;', '{"', log)
                log = re.sub(r'&quot;}$', '"}', log)
                len_to_consider = len('&quot;exception&quot;:')
                index_before_exception = log.rfind('&quot;exception&quot;') + len_to_consider
                log_before_exception = log[0: index_before_exception]
                log_before_exception = re.sub(r'&quot;:', '":', log_before_exception)
                log_before_exception = re.sub(r':&quot;', ':"', log_before_exception)
                log_before_exception = re.sub(r'&quot;,', '",', log_before_exception)
                log_before_exception = re.sub(r',&quot;', ',"', log_before_exception)
                log_before_exception = re.sub(r':{&quot;', ':{"', log_before_exception)
                log_before_exception = re.sub(r'&quot;},', '"},', log_before_exception)

                log_after_excepion = log[index_before_exception: -2].replace('"', '&quot;')
                total_log = json.loads(log_before_exception + '"' + log_after_excepion + '"' + '}')
                return total_log
            except:
                return 
        


    def find_level2_exception_classes(self, log: str, message: str):
        """
        Find level2_exception_classes
        
        :param: log: raw log data from kibana

        :return: string of formatted log
        """ 
        log = log.lower()
        log_before_quot = log[: log.find(":")]
        main_exception = ""
        if "&quot;" in log_before_quot:
            main_exception = log_before_quot[log_before_quot.find("&quot;") + len("&quot;"):]
        else:
            main_exception = log_before_quot
        if not main_exception or log == 'unknown':
            if "nested exception is" in message:
                message_after_nested_exception = message[message.find("nested exception is") + len("nested exception is"): ]
                main_exception = message_after_nested_exception[: message_after_nested_exception.find(":")]

        return ''.join(main_exception.split())



    def find_level1_exception_message_filtered(self, message: str):
        """
        Find level1_exception_message
        
        :param: message: raw log data from kibana

        :return: string of formatted log
        """
        message = message.lower()
        cleaned_message = re.sub(r"\[.*?\]", "", message)
        cleaned_message = re.sub(r"(\w+-\w+)|-+", "", cleaned_message)
        target_index_id = cleaned_message.find("with id")
        if target_index_id != -1:
            cleaned_message = cleaned_message[: target_index_id]
        target_index_colon = cleaned_message.find(":")
        if target_index_colon != -1:
            cleaned_message = cleaned_message[: target_index_colon]
        else:
            target_index_period = cleaned_message.rfind(".")
            if target_index_period != -1:
                cleaned_message = cleaned_message[: target_index_period]

        # further clean: remove pod id and execution id
        cleaned_message = re.sub(r"[^a-zA-Z0-9._-]+", " ", cleaned_message)
        if 'pod' in cleaned_message:
            pod = cleaned_message.rfind('pod')
            return ' '.join(cleaned_message[:pod + len('pod')].split())
        elif 'threw exception when executing' in cleaned_message:
            execution = cleaned_message.find('execution')
            threw_exception = cleaned_message.find('threw exception when executing')
            total_cleaned_message = cleaned_message[0: execution + len('execution')] + ' ' + cleaned_message[threw_exception:]
            return ' '.join(total_cleaned_message.split())

        return ' '.join(cleaned_message.lower().split())
    


    def find_exception_message_detailed(self, message: str):
        """
        Find exception_message_detailed
        
        :param: message: raw log data from kibana

        :return: string of formatted log
        """
        message = message.lower()
        cleaned_message = re.sub(r"[^a-zA-Z0-9-_]+", " ", message)
        return ' '.join(cleaned_message.split())



    def find_level0_entry_details(self, timestamp: str, pod_id: str, host: str, container_name: str, deploy_time: str, log: str):
        """
        Find level0_entry_details
        
        :param: timestamp: timestamp of this log
        :param: pod_id: pod_id of this log
        :param: host: host of this log
        :param: container_name: container_name of this log
        :param: deploy_time: deploy_time of this log
        :param: log: log of this log

        :return: string of formatted log
        """
        log_json = {'timestamp': timestamp, 
                    'pod_id': pod_id, 'host': host, 
                    'container_name': container_name, 
                    'deploy_time': deploy_time,
                    'log': log}
        return log_json



    def find_level3_exception_causes(self, log: str):
        """
        Find level3_exception_causes
        
        :param: message: raw log data from kibana

        :return: string of formatted log
        """
        log = log.lower()
        try:
            if 'cause:' in log:  
                log_after_cause = log[log.find('cause:') + len('cause:'): ]
                log_before_colon = log_after_cause[: log_after_cause.find(':')]
                return log_before_colon
        except:
            return 'unknown'
        


    def clean_data(self):
        """
        Main function to clean log, call this function will clean everything
        
        :return: a cleaned dataframe  
        """
        try:
            df_all = self.json_to_dataframe()
            df_all['log'] = df_all['log'].apply(lambda x: self.format_log(x))
            df_all = df_all.dropna(axis=0, subset=['log']).reset_index()
            df_all_split = pd.json_normalize(df_all['log']).fillna('unknown')
            data = pd.concat([df_all_split, df_all.drop('log', axis = 1)], axis=1)

            data['level0_entry_details'] = data.apply(lambda x: self.find_level0_entry_details(x.timestamp, x.pod_id, x.host, x.container_name, x.deploy_time, x.exception), axis = 1)
            data['exception_message_detailed'] = data['message'].apply(lambda x: self.find_exception_message_detailed(x))
            data['level1_exception_message_filtered'] = data['message'].apply(lambda x: self.find_level1_exception_message_filtered(x))
            data['level2_exception_classes'] = data.apply(lambda x: self.find_level2_exception_classes(x.exception, x.message), axis = 1)
            data['level3_exception_causes'] = data['message'].apply(lambda x: self.find_level3_exception_causes(x))
            data = data.rename({'logger': 'exception_logger'}, axis = 1)
            data = data.fillna('unknown')
            data['created_at'] = datetime.datetime.now()
            return data
        
        except:
            return pd.DataFrame()



if __name__ == '__main__':
    cleaned_data = DataCleansing(json_data).clean_data()
