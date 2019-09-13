import json
from pymongo import MongoClient


def get_config():
    with open('setting.json', 'r', encoding='utf-8') as file:
        setting = json.loads(file.read())
    return setting


config = get_config()
client = MongoClient(config["mongodb"]['address'], config["mongodb"]['port'])
