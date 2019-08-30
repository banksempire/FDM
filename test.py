
import fdm
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
from datetime import datetime

client = MongoClient("192.168.56.1", 27017)

source = client.tushareCache.dailyAdjFactor_single.find

fdm.Tushare(client)
