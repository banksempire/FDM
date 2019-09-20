from .config import config


def change_client(address, port):
    from . import client
    from pymongo import MongoClient
    client.client = MongoClient(address, port)
