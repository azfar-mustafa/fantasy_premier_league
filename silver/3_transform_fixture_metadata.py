import json
import os
import pytz
from datetime import datetime
import duckdb
import shutil


def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp


def check_dictionary_key(current_season_past_fixture):
     consistent_keys = all(d.keys() == current_season_past_fixture[0].keys() for d in current_season_past_fixture)
     if consistent_keys:
        print("All dictionaries have consistent keys")
        return True
     else:
        print("Dictionaries have different keys")
        return False


def create_history_fixture_json_file(json_data, folder_path, file_name):
    silver_file_full_path = os.path.join(folder_path, file_name)
    with open(silver_file_full_path, "w") as json_file:
        json.dump(json_data, json_file, indent=4)


def create_folder(folder_name):
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)
        print(f"Folder '{folder_name}' created successfully.")
        return folder_name
    else:
        print(f"Folder '{folder_name}' already exists.")
        return None


def convert_json_to_parquet(silver_folder_path, json_file, parquet_file):
    silver_json_file_full_path = os.path.join(silver_folder_path, json_file)
    silver_parquet_file_full_path = os.path.join(silver_folder_path, parquet_file)
    duckdb.sql(f"CREATE TABLE past AS SELECT * FROM read_json_auto('{silver_json_file_full_path}')")
    duckdb.sql(f"COPY (SELECT * FROM past) TO '{silver_parquet_file_full_path}' (FORMAT PARQUET)")


def delete_json_file(silver_folder_path, json_file_name):
    silver_json_file_full_path = os.path.join(silver_folder_path, json_file_name)
    if os.path.exists(silver_json_file_full_path):
        os.remove(f"{silver_json_file_full_path}")
        print("Json file is deleted")


def delete_silver_folder(folder_path):
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        print("Folder is deleted")


if __name__ == "__main__":
    all_dict = []
    current_date = convert_timestamp_to_myt()
    bronze_file_directory = f"C:/Users/khair/project/fantasy_premier_league/data/bronze/fantasy_premier_league/current_season_history_data/{current_date}/"
    silver_file_directory = f"C:/Users/khair/project/fantasy_premier_league/data/silver/fantasy_premier_league/current_season_history_data/{current_date}/"
    silver_json_file = f"current_season_history_fixture_{current_date}.json"
    silver_parquet_file = f"current_season_history_fixture_{current_date}.parquet"
    
    delete_silver_folder(silver_file_directory)
    create_folder(silver_file_directory)
    for filename in os.listdir(bronze_file_directory):
        if filename.endswith('.json'):
            file_path = os.path.join(bronze_file_directory, filename)
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                current_season_past_fixture = data['history']
                all_dict.extend(current_season_past_fixture)

    create_history_fixture_json_file(all_dict, silver_file_directory, silver_json_file)
    convert_json_to_parquet(silver_file_directory, silver_json_file, silver_parquet_file)
    delete_json_file(silver_file_directory, silver_json_file)
    