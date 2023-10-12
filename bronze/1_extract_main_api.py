import requests
import json
from datetime import datetime
import pytz
import os
import configparser
import shutil


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder = config['file_path']['bronzefolder']
    return bronze_folder


def fetch_data(website_url):
    try:
        player_team_detail_url = website_url
        response = requests.get(player_team_detail_url, stream=True, timeout=2)
        data = response.json()
        return data['events'], data['teams'], data['elements'], data['element_types']
    except Exception as e:
        print(e)
        return None
    

def create_data(file_path, file_name_json, data):
    with open(f"{file_path}\{file_name_json}", 'w') as file:
        json.dump(data, file, indent=4)
    print(f"{file_path} data is created")


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
        print(f"Existing {folder_path} is deleted")
    os.makedirs(folder_path)
    return folder_path


#to check if folder exists, delete folder if exists 

if __name__ == "__main__":
    try:
        metadata = ['events_metadata', 'teams_metadata', 'player_metadata', 'position_metadata']
        current_timestamp = convert_timestamp_to_myt()
        url_list = 'https://fantasy.premierleague.com/api/bootstrap-static/'
        bronze_folder = get_file_path()
        events, teams, player, position = fetch_data(url_list)
        zipped_api = zip(metadata, fetch_data(url_list))
        for file_name,data in zipped_api:
            folder_path = create_directory(current_timestamp, file_name, bronze_folder)
            file_name_json = f"{file_name}_{current_timestamp}.json"
            create_data(folder_path, file_name_json, data)
    except Exception as e:
        print(e)