from datetime import datetime, timedelta

import pandas as pd
from pandas import DataFrame

import fdm

wsd = fdm.Wind().wsd()
sec = fdm.Wind().sector_constituent()

date = datetime(2019, 11, 20)
# All china shares
alla = sec.query('a001010900000000', date)
delist = sec.query('a001010m00000000', date)
alla = alla.append(delist)
alla = alla.reset_index().drop(columns='index')
STARTDATE = datetime(2000, 1, 1)
ENDDATE = datetime(2019, 10, 31)
codes = list(alla['wind_code'])

starttime = datetime.now()
close = wsd.query(codes, 'close||PriceAdj=B',
                  STARTDATE, ENDDATE)
#close = wsd.query(['000001.sz','000002.sz'],'close',STARTDATE,ENDDATE, skip_update = True)
endtime = datetime.now()
print(endtime - starttime)
print(close.shape)
