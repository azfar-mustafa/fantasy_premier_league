import json
import time
import requests
import pytz
import os
from requests.exceptions import RequestException
from datetime import datetime
import configparser
import logging


def configure_logging():
    log_format = "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
    log_file_current_timestamp = time.strftime("%Y%m%d")
    log_folder_path = "C:/Users/khair/project/fantasy_premier_league/log/"    
    log_file_name = f"fantasy_premier_league_{log_file_current_timestamp}.log"
    log_file_path = f"{log_folder_path}{log_file_name}"
    logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.INFO, format=log_format)


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder_player_data = config['file_path']['BronzeFolderPlayerData']
    bronze_folder_player_metadata = config['file_path']['BronzeFolderPlayerMetadata']
    return bronze_folder_player_data, bronze_folder_player_metadata


def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp

def read_file(file_path):
    with open(file_path, "r") as json_file:
        data = json.load(json_file)
    return data


def get_player_data(player_index):
    url = f"https://fantasy.premierleague.com/api/element-summary/{player_index}/"
    response = requests.get(url, timeout=60)
    if response.status_code == 200:
        player_data = response.json()
        return player_data
    else:
        logging.error(f"Request failed with status code: {response.status_code}")
        return None
    

def create_folder(folder_date, bronze_folder_player_data):
    folder_name = f"{bronze_folder_player_data}/{folder_date}"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        logging.info(f"Folder '{folder_name}' created successfully.")
        return folder_name
    else:
        logging.info(f"Folder '{folder_name}' already exists.")
        return None

    


def create_player_file(player_index, player_data, file_date, folder_name):
    player_file_name = f"{folder_name}/{player_index}_{file_date}.json" 
    with open(player_file_name, 'w') as file: 
        json.dump(player_data, file, indent=4)
        logging.info(f"{player_file_name} is created")
        time.sleep(5)
 
 #to check if folder exists, delete folder if exists 

if __name__ == "__main__":
    try:
        configure_logging()
        bronze_folder_player_data, bronze_folder_player_metadata = get_file_path()
        current_date = convert_timestamp_to_myt()
        file_name = f"player_metadata_{current_date}.json"
        player_metadata_path = f"{bronze_folder_player_metadata}/{current_date}/{file_name}"
        main_json_file = read_file(player_metadata_path)
        folder_name = create_folder(current_date, bronze_folder_player_data)

        for id_player in main_json_file:
            player_id = id_player.get("id")
            logging.info(f"Current player id: {player_id}")
            player_data = get_player_data(player_id)
            create_player_file(player_id, player_data, current_date, folder_name)
            
    except  RequestException as e:
        logging.error(f"An error occured {e}")