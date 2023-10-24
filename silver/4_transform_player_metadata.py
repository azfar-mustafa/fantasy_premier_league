import duckdb
import json
import os
import pytz
from datetime import datetime
import duckdb
import shutil
import configparser
import logging
import time


def configure_logging():
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    log_file_current_timestamp = time.strftime("%Y%m%d")
    log_folder_path = "C:/Users/khair/project/fantasy_premier_league/log/"    
    log_file_name = f"4_transform_player_metadata_{log_file_current_timestamp}.log"
    log_file_path = f"{log_folder_path}{log_file_name}"
    logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.INFO, format=log_format)


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder_player_metadata = config['file_path']['BronzeFolderPlayerMetadata']
    silver_folder_player_metadata = config['file_path']['SilverFolderPlayerMetadata']
    return bronze_folder_player_metadata, silver_folder_player_metadata


def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp


def create_player_parquet_file(bronze_folder_path, bronze_file_name, silver_folder_path, silver_file_name):
    bronze_json_file_full_path = os.path.join(bronze_folder_path, bronze_file_name)
    silver_parquet_file_full_path = os.path.join(silver_folder_path, silver_file_name)
    duckdb.sql(f"CREATE TABLE player_metadata AS SELECT * FROM read_json_auto('{bronze_json_file_full_path}')")
    duckdb.sql("CREATE TABLE player_metadata_trans_1 AS SELECT *, CONCAT(first_name, ' ', second_name) as full_name, now_cost/10 as latest_price FROM player_metadata")
    duckdb.sql(f"COPY (SELECT * FROM player_metadata_trans_1) TO '{silver_parquet_file_full_path}' (FORMAT PARQUET)")


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        logging.info(f"Folder '{folder_name}' created successfully.")
        return folder_name
    else:
        logging.info(f"Folder '{folder_name}' already exists.")
        return None
    

#Add new column to combine first name & second name

def player_metadata_transformation(silver_folder_path, silver_file_name):
    silver_parquet_file_full_path = os.path.join(silver_folder_path, silver_file_name)
    duckdb.sql(f"CREATE TABLE player_metadata AS SELECT * FROM read_parquet('{silver_parquet_file_full_path}')")
    print(duckdb.sql("SELECT *, CONCAT(first_name, ' ', second_name) FROM player_metadata").show())
    

def delete_silver_folder(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        logging.info("Folder is deleted")


if __name__ == "__main__":
    configure_logging()
    bronze_folder_player_metadata, silver_folder_player_metadata = get_file_path()
    current_date = convert_timestamp_to_myt()
    bronze_file_directory = f"{bronze_folder_player_metadata}/{current_date}/"
    silver_file_directory = f"{silver_folder_player_metadata}/{current_date}/"
    bronze_json_file = f"player_metadata_{current_date}.json"
    silver_parquet_file = f"player_metadata_{current_date}.parquet"

    delete_silver_folder(silver_file_directory)
    create_folder(silver_file_directory)
    create_player_parquet_file(bronze_file_directory, bronze_json_file, silver_file_directory, silver_parquet_file)