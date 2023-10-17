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
    log_filename = f"5_transform_team_metadata{log_file_current_timestamp}.log"
    logging.basicConfig(filename=log_filename, encoding='utf-8', level=logging.INFO, format=log_format)


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder_team_metadata = config['file_path']['BronzeFolderTeamMetadata']
    silver_folder_team_metadata = config['file_path']['SilverFolderTeamMetadata']
    return bronze_folder_team_metadata, silver_folder_team_metadata


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
    duckdb.sql(f"CREATE TABLE past AS SELECT * FROM read_json_auto('{bronze_json_file_full_path}')")
    duckdb.sql(f"COPY (SELECT * FROM past) TO '{silver_parquet_file_full_path}' (FORMAT PARQUET)")


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        logging.info(f"Folder '{folder_name}' created successfully.")
        return folder_name
    else:
        logging.info(f"Folder '{folder_name}' already exists.")
        return None
    

#Add new column to combine first name & second name

if __name__ == "__main__":
    bronze_folder_team_metadata, silver_folder_team_metadata = get_file_path()
    current_date = convert_timestamp_to_myt()
    bronze_file_directory = f"{bronze_folder_team_metadata}/{current_date}/"
    silver_file_directory = f"{silver_folder_team_metadata}/{current_date}/"
    bronze_json_file = f"teams_metadata_{current_date}.json"
    silver_parquet_file = f"teams_metadata_{current_date}.parquet"
    create_folder(silver_file_directory)
    create_player_parquet_file(bronze_file_directory, bronze_json_file, silver_file_directory, silver_parquet_file)