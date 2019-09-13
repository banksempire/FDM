from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient

from .metaclass import _CollectionBase, _DbBase
import fdm


class CleanData(_DbBase):
    '''Database that store cleared data. 
    All colletion in this database has index that can provide fast query.'''

    def __init__(self, client: MongoClient, settingname='cleandata'):
        super().__init__(client, settingname)

    def price(self):
        return self._inti_col(Price, 'price')


class Price(_CollectionBase):
    '''Collection of market price.
    Collection scheme:
    |code|date|open|high|low|close|vwap|adj_factor|'''

    def _keyring(self, key: str):
        '''Function that return the correct data source given source.'''
        def _tushare(startdate, enddate) -> DataFrame:
            # Get raw pricing data
            client = self.get_client()
            pricesource = fdm.Tushare(client).daily_price()
            df = pricesource.query(startdate=startdate, enddate=enddate, fields=[
                'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount'])
            if df.empty:
                return df
            # Data Preprocessing
            df.index = [df['ts_code'], df['trade_date']]
            del df['ts_code']
            del df['trade_date']
            df['vwap'] = df['amount']/df['vol']*10
            del df['amount']
            del df['vol']
            # Get adjust factor
            pricesource = fdm.Tushare(client).daily_adj()
            adf = pricesource.query(startdate=startdate, enddate=enddate)
            adf.index = [adf['ts_code'], adf['trade_date']]
            # Data Preprocessing
            del adf['ts_code']
            del adf['trade_date']

            df = df.join(adf)
            del adf

            for i in ['open', 'high', 'low', 'close', 'vwap']:
                df[i] = df[i]*df['adj_factor']
            df.reset_index(inplace=True)
            df.rename(columns={'ts_code': 'code',
                               'trade_date': 'date'}, inplace=True)
            return df

        keyring = {
            'tushare': _tushare
        }
        return keyring[key.lower()]

    def update(self, source: str = 'tushare'):
        '''Update database to the latest from source'''
        function = self._keyring(source)
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
        function = self._keyring(source)
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
