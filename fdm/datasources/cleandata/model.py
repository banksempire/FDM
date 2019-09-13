from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient

from fdm.datasources.metaclass import _CollectionBase, _DbBase
import fdm
from .feeders import feeder_funcs


class CleanData(_DbBase):
    '''Database that store cleared data. 
    All colletion in this database has index that can provide fast query.'''

    def price(self):
        return self._inti_col(Price)


class Price(_CollectionBase):
    '''Collection of market price.
    Collection scheme:
    |code|date|open|high|low|close|vwap|adj_factor|'''

    def update(self, source: str = 'tushare'):
        '''Update database to the latest from source'''
        function = feeder_funcs[source]
        enddate = datetime.now()
        lastdate = self.interface.lastdate()
        if lastdate is not None:
            if lastdate < enddate:
                df = function(lastdate, enddate)
        self.interface.insert_many(df)
        return 0

    def rebuild(self, source: str = 'tushare'):
        '''Rebuild database from source'''
        # Clean database
        self.interface.drop()
        # Get the correct data fetching function base on class name of "source"
        function = feeder_funcs[source]
        startdate = datetime(1990, 1, 1)
        enddate = datetime.now()
        # Fill in entry by batch
        while startdate <= enddate:
            df = function(startdate, startdate+timedelta(days=1000))
            self.interface.insert_many(df)
            startdate = startdate+timedelta(days=1000)
        # Fill in entry for residual date
        self.update(source)
        # create index
        self.interface.create_indexs(['code', 'date'])
        return 0

    def list_code_names(self) -> list:
        '''Get stock code in the collection.'''
        return self.interface.list_code_names()

    def ror(self, date: datetime, codes: list = None, freq='B', price='close'):
        for df in self.interface.rolling_query(2, enddate=date, fields=['code', 'date', price], freq=freq, ascending=False):
            tdf = df.pivot(index='date', columns='code',
                           values=price).sort_index()
            #tdf = tdf.pct_change().dropna(how='all')
            return tdf
