import json

file_path = "player_data/1.json"
with open(file_path, "r") as json_file:
    data = json.load(json_file)
    print(data['history'][0].keys())