import json
import time
import requests
import pytz
import os
from requests.exceptions import RequestException
from datetime import datetime

def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp

def read_file():
    file_path = "data/raw/elements_05092023.json"  # Replace with your file path
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
        print(f"Request failed with status code: {response.status_code}")
        return None
    

def create_folder(folder_date):
    folder_name = f"data/raw/player_data/{folder_date}"
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Folder '{folder_name}' created successfully.")
        return folder_name
    else:
        print(f"Folder '{folder_name}' already exists.")
        return None

    


def create_player_file(player_index, player_data, file_date, folder_name):
    player_file_name = f"{folder_name}/{player_index}_{file_date}.json" 
    with open(player_file_name, 'w') as file: 
        json.dump(player_data, file, indent=4)

        print(f"{player_file_name} is created")
        time.sleep(5)
 

if __name__ == "__main__":
    try:
        main_json_file = read_file()
        current_date = convert_timestamp_to_myt()
        folder_name = create_folder(current_date)
        for id_player in main_json_file:
            player_id = id_player.get("id")
            print(f"Current player id: {player_id}")
            player_data = get_player_data(player_id)
            create_player_file(player_id, player_data, current_date, folder_name)
    except  RequestException as e:
        print(f"An error occured {e}")