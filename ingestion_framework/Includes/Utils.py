from pyspark.sql import DataFrame, Row, SparkSession
from typing import Iterable, Optional
# from google.cloud import storage
from pathlib import Path
import asyncio
from confluent_kafka import Consumer, TopicPartition
import time
from datetime import datetime

import json 
import os    

def get_secret(key: str):
    """
    Returns secret value for a given secret key. Raises exception if the provided key does not exist in secrets.
    For more information, ref: https://github.com/shipt/lakehouse-ingestion-framework#secrets
    """    
    f = open('/secrets/secrets.json')
    secrets = json.load(f)
    if key in secrets.keys():
        return secrets.get(key)
    else:
        print(f"Provided key '{key}' does not exist in secrets.")
        raise KeyError

def split_or_null(combined_string: str, delimiter: str = ';') -> Optional[Iterable[str]]: 
    """
    Returns an iterable of string values given a string containing one or more items separated by a delimiter.
    The default delimiter (semi-colon) will be used when the delimiter is not explicitly passed to the function 
    
        Parameters:
            combined_string: str
                A delimited string containing one or many items to be split
            delimiter: str, optional
                An optional delimiter to use for splitting the combined_string. Default value is ';' 
        Returns:

            Optional[Iterable[str]]: Returns an interable of string values if the combined_string value is not null, 
            otherwise returns None

    """
    if combined_string is None: 
        return None 
    tmp_array = map(lambda x: x.strip(), combined_string.split(delimiter))
    return None if tmp_array == [''] else tmp_array


def try_get_column_str(row: Row, col_name: str) -> Optional[str]:
    """
    Provides a safe way to return the string value stored in a row's column if the column name exists in the row, otherwise None is returned.
    Use if you do not want to raise an error if the column name does not exist.
    
        Parameters:
            row: Row
                A row from a dataframe
            col_name: str
                The name of the column in the row that you wish to return
        Returns:
            Optional[str]: Returns the string value of a column if it exists, otherwise None is returned
    """
    try:
        return row[col_name].strip()
    except:
        return None

def try_get_column_int(row: Row, col_name: str) -> Optional[int]:
    """
    Provides a safe way to return the integer value stored in a row's column if the column name exists in the row, otherwise None is returned.
    Use if you do not want to raise an error if the column name does not exist or if you want to return None if the value cannot be cast to an integer.
    
        Parameters:
            row: Row
                A row from a dataframe
            col_name: str
                The name of the column in the row that you wish to return
        Returns:
            Optional[int]: Returns the int value of a column if it exists, otherwise None is returned
    """
    try:
        return int(row[col_name])
    except:
        return None


def get_max_bytes_per_trigger(max_bytes_per_trigger:Optional[str]):
        """
        function to get either the maxBytesPerTrigger value speicified in the metadata table or return the default value of 8g
        """
        if max_bytes_per_trigger is not None:
            return max_bytes_per_trigger
        else:
            return '8g'


def get_trigger_interval(trigger_interval:Optional[str]):
    """
    Sets the default trigger interval time if not specified in the metadata table, otherwise the trigger interval supplied by the metadata table is returned
    """
    return trigger_interval if trigger_interval is not None else '120 seconds'

def get_gs_bucket_object(object_path):
    path = Path(object_path)
    path_parts = [p for p in path.parts]
    bucket_name = path_parts[1]
    object_name = '/'.join(path_parts[2:])
    return [bucket_name, object_name]

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """
        Downloads a blob from the bucket to a local path.
        local directories must exit for the path
        Example objectURI: gs://bucket_name/dir1/dir2/file.json
        parameters:
            bucket_name:  
                The base bucket name (i.e. using the exampleURI above, the bucket name is 'bucket_name' without the gs://)
            source_blob_name:
                The remainder of the objectURI after the bucket name, but without the leading backslash.
                    i.e. using the exampleURI above the source_bloc_name is dir1/dir2/file.json
            destination_file_name:
                The full local filepath including the filename (i.e. /dir1/dir2/file.json)
                ** The directories need to exist on the local filesystem.  In the example used, the /dir1/dir2 
                   directories need to exist in the filesystem already
    """
    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

def get_max_offsets_per_trigger(maxOffsetsPerTrigger:Optional[int]):
    """
    function to return the maxOffsetsPerTrigger option specified in the metadata table for each pipeline.
    If no value was provided or the value can't be parsed as an int, it will return the default value of 1000.
    The maxOffsetsPerTrigger option is an inbound parameter for protecting against large spikes in volume causing failures
    and long delays in processing
    """
    default_size = 1000
    if maxOffsetsPerTrigger:
        try:
            return int(maxOffsetsPerTrigger)
        except:
            return default_size
    else:
        return default_size

def validate_date_string(date_str):
    """
    function to validate the date string can be converted to a date with YYYY-MM-DD HH:MM:SS format.
    Returns a list with the date value as the first element and the error message as the second.  
    If no value was provided, the function returns a None value with no error message.
    If the string can be converted to a datetime field, the function will return the string date and an empty string for the error message.
    If the string cannot be converted, the function will return a None value and the exception, so the exxception can be logged
    """
    format_str = '%Y-%m-%d %H:%M:%S'
    format_str_trunc = '%Y-%m-%d'
    if date_str is None:
        return [None, ""]
    try:
        converted_date = datetime.strptime(date_str, format_str)
        return [date_str, ""]
    except:
        try:
            converted_date = datetime.strptime(date_str, format_str_trunc)
            return [date_str, ""]
        except Exception as e:
            return [None, e] 



