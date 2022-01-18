import datetime
from configparser import ConfigParser


def to_timestamp(time):
    try:
        return datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=datetime.timezone.utc).timestamp()
    except:
        return datetime.datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=datetime.timezone.utc).timestamp()


def load_config_table(region_name):
    try:
        config_parser = ConfigParser()
        config_parser.read("/Users/i543474/Documents/IOD/code/config.ini")
        str_existing_exception = region_name.upper() + '_EXISTING_EXCEPTION_TABLE_NAME'
        str_new_exception = region_name.upper() + '_NEW_IDENTIFIED_EXCEPTION_TABLE_NAME'

        WEU_EXISTING_EXCEPTION_TABLE_NAME = config_parser.get("blob-storage", str_existing_exception)
        WEU_NEW_IDENTIFIED_EXCEPTION_TABLE_NAME = config_parser.get("blob-storage", str_new_exception)

        return WEU_EXISTING_EXCEPTION_TABLE_NAME, WEU_NEW_IDENTIFIED_EXCEPTION_TABLE_NAME
    except:
        return 


def load_config(header, name):
    try:
        config_parser = ConfigParser()
        config_parser.read("/Users/i543474/Documents/IOD/code/config.ini")
        return config_parser.get(header, name)
    except:
        return 
