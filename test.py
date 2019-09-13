import json
from datetime import datetime


import fdm


db = fdm.Tushare()['daily_basic']
db.update()
