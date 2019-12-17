from pymongo import MongoClient
from .config import config
client = MongoClient(config["mongodb"]['address'], config["mongodb"]['port'])
