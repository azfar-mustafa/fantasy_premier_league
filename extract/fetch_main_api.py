import requests
import json
from datetime import datetime
import pytz


def fetch_data(website_url):
    try:
        player_team_detail_url = website_url
        response = requests.get(player_team_detail_url, stream=True, timeout=2)
        data = response.json()
        return data['events'], data['teams'], data['elements']
    except Exception as e:
        print(e)
        return None
    

def create_data(file_path, data):
    with open(file_path, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"{file_path} data is created")


def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp


if __name__ == "__main__":
    try:
        current_timestamp = convert_timestamp_to_myt()
        url_list = 'https://fantasy.premierleague.com/api/bootstrap-static/'
        events, teams, elements = fetch_data(url_list)
        create_data(f"data/raw/events_{current_timestamp}.json", events)
        create_data(f"data/raw/teams_{current_timestamp}.json", teams)
        create_data(f"data/raw/elements_{current_timestamp}.json", elements)
    except Exception as e:
        print(e)