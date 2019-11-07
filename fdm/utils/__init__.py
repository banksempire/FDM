from .config import config
from .maintainer import update_all


def change_client(address, port):
    from . import client
    from pymongo import MongoClient
    client.client = MongoClient(address, port)
