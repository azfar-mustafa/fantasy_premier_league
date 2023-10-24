import requests
import json
from datetime import datetime
import pytz
import os
import configparser
import shutil
import logging
import time


url_list = 'https://fantasy.premierleague.com/api/bootstrap-static/'


def configure_logging():
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    log_file_current_timestamp = time.strftime("%Y%m%d")
    log_folder_path = "C:/Users/khair/project/fantasy_premier_league/log/"
    log_file_name = f"1_extract_main_api_{log_file_current_timestamp}.log"
    log_file_path = f"{log_folder_path}{log_file_name}"
    logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.INFO, format=log_format)


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder = config['file_path']['bronzefolder']
    return bronze_folder


def fetch_data(website_url):
    try:
        player_team_detail_url = website_url
        response = requests.get(player_team_detail_url, stream=True, timeout=2)
        response.raise_for_status()
        data = response.json()
        return data['events'], data['teams'], data['elements'], data['element_types']
    except Exception as e:
        logging.error(f"An error occured: {e}")
        return None
    

def create_data(file_path, file_name_json, data):
    with open(f"{file_path}\{file_name_json}", 'w') as file:
        json.dump(data, file, indent=4)
    logging.info(f"{file_path} data is created")


def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp


def create_directory(folder_timestamp, data_type, bronze_folder):
    folder_path = os.path.join(bronze_folder, data_type, folder_timestamp)
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        logging.info(f"Existing {folder_path} is deleted")
    os.makedirs(folder_path)
    return folder_path


#to check if folder exists, delete folder if exists 

if __name__ == "__main__":
    try:
        configure_logging()
        metadata = ['events_metadata', 'teams_metadata', 'player_metadata', 'position_metadata']
        current_timestamp = convert_timestamp_to_myt()
        bronze_folder = get_file_path()
        data = fetch_data(url_list)
        if data:
            zipped_api = zip(metadata, data)
            for file_name,data in zipped_api:
                folder_path = create_directory(current_timestamp, file_name, bronze_folder)
                file_name_json = f"{file_name}_{current_timestamp}.json"
                create_data(folder_path, file_name_json, data)
    except Exception as e:
        logging.error(f"An error occured: {e}")