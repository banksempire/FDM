import json


def get_config():
    with open('setting.json', 'r', encoding='utf-8') as file:
        setting = json.loads(file.read())
    return setting


config = get_config()
