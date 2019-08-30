import fdm
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
from datetime import datetime

client = MongoClient("192.168.56.1", 27017)
tdb = fdm.TempDB(client)['tdb']

tsubcol = fdm.TempDB(client)['subcol']
'''df = tdb.query()
print(df.columns)
tsubcol.insert_many_subcol(df)'''

tsubcol.drop()
