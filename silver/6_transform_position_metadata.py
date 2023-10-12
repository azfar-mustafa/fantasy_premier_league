import duckdb
import json
import os
import pytz
from datetime import datetime
import duckdb
import shutil
import configparser


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder_element_types = config['file_path']['BronzeFolderPositionMetadata']
    silver_folder_player_position = config['file_path']['SilverFolderPlayerPosition']
    return bronze_folder_element_types, silver_folder_player_position


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
        print(f"Folder '{folder_name}' created successfully.")
        return folder_name
    else:
        print(f"Folder '{folder_name}' already exists.")
        return None
    

if __name__ == "__main__":
    bronze_folder_element_types, silver_folder_player_position = get_file_path()
    current_date = convert_timestamp_to_myt()
    bronze_file_directory = f"{bronze_folder_element_types}/{current_date}/"
    silver_file_directory = f"{silver_folder_player_position}/{current_date}/"
    bronze_json_file = f"position_metadata_{current_date}.json"
    silver_parquet_file = f"player_position_{current_date}.parquet"
    create_folder(silver_file_directory)
    create_player_parquet_file(bronze_file_directory, bronze_json_file, silver_file_directory, silver_parquet_file)