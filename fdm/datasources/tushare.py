import json
from time import sleep
from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient


import tushare as ts

from fdm.utilities import retry
from .metaclass import _CollectionBase, _DbBase


class _TushareCollectionBase(_CollectionBase):

    def _rebuild(self, download_function):
        # Drop all data in collection
        self.col.drop()
        print('{0} droped'.format(self.col.full_name))
        # Inititalize data source
        pro = ts.pro_api()
        # Get stock list
        stock_list = DataFrame()
        for status in "LDP":
            stock_list = stock_list.append(pro.stock_basic(list_status=status))
        # Download data for each stock code
        for _, value in stock_list.iterrows():
            code = value['ts_code']
            startdate = datetime(1990, 1, 1)
            today = datetime.now()
            while startdate < today:
                enddate = min(startdate+timedelta(4000), today)
                df = download_function(code, pro, startdate, enddate)
                startdate = startdate+timedelta(4001)
                self.col.insert_many(df)
                sleep(0.6)
            print('Code: {0} downloaded.'.format(code))
            sleep(0.6)
        return 0

    def _update(self, download_function):
        # Inititalize data source
        pro = ts.pro_api()
        # Get last date in DB
        lastdate = self.col.lastdate('trade_date')
        # Generate date range business day only
        daterange = pd.date_range(start=lastdate+timedelta(1),
                                  end=datetime.now(), freq="B")
        print('Total {0} data points need to be downloaded.'.format(
            len(daterange)))
        # Download data for each day
        for i in daterange:
            date = i.strftime('%Y%m%d')
            df = download_function(date, pro)
            self.col.insert_many(df)
            print('Date: {0} downloaded.'.format(date))
            sleep(0.6)
        return 0


class Tushare(_DbBase):
    def __init__(self, client: MongoClient, settingname='tushare'):
        super().__init__(client, settingname)
        ts.set_token(self.setting['others']['token'])

    def __getitem__(self, key) -> _CollectionBase:
        keyring = {
            'daily': self.daily_price,
            'dailyBasic': self.daily_basic,
            'dailyAdjFactor': self.daily_adj
        }
        return keyring[key]()

    def daily_price(self):
        colName = self.setting['DBSetting']['colSetting']['daily']
        col = self.db[colName]
        return DailyPrice(col)

    def daily_basic(self):
        colName = self.setting['DBSetting']['colSetting']['dailyBasic']
        col = self.db[colName]
        return DailyBasic(col)

    def daily_adj(self):
        colName = self.setting['DBSetting']['colSetting']['dailyAdjFactor']
        col = self.db[colName]
        return DailyAdjFactor(col)


class DailyBasic(_TushareCollectionBase):

    def rebuild(self):
        @retry()
        def download_data(code, pro, start_date, end_date):
            df = pro.daily_basic(ts_code=code,
                                 start_date=start_date.strftime('%Y%m%d'),
                                 end_date=end_date.strftime('%Y%m%d'))
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], format='%Y%m%d')
            return df
        print('Rebuild daily basic cache.')
        self._rebuild(download_data)
        return 0

    def update(self):
        @retry()
        def download_data(date, pro):
            df = pro.daily_basic(trade_date=date)
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], format='%Y%m%d')
            return df

        self._update(download_data)
        return 0


class DailyPrice(_TushareCollectionBase):

    def rebuild(self):
        @retry()
        def download_data(code, pro, start_date, end_date):
            df = pro.daily(ts_code=code,
                           start_date=start_date.strftime('%Y%m%d'),
                           end_date=end_date.strftime('%Y%m%d'))
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], format='%Y%m%d')
            return df
        print('Rebuild daily price cache.')
        self._rebuild(download_data)
        return 0

    def update(self):
        @retry()
        def download_data(date, pro):
            df = pro.daily(trade_date=date)
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], format='%Y%m%d')
            df['adj_factors'] = pro.adj_factor(trade_date=date)['adj_factor']
            return df

        self._update(download_data)
        return 0


class DailyAdjFactor(_TushareCollectionBase):

    def rebuild(self):
        @retry()
        def download_data(code, pro, start_date, end_date):
            df = pro.adj_factor(ts_code=code,
                                start_date=start_date.strftime('%Y%m%d'),
                                end_date=end_date.strftime('%Y%m%d'))
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], format='%Y%m%d')
            return df
        print('Rebuild daily adjfactor cache.')
        self._rebuild(download_data)
        return 0

    def update(self):
        @retry()
        def download_data(date, pro):
            df = pro.adj_factor(trade_date=date)
            df['trade_date'] = pd.to_datetime(
                df['trade_date'], format='%Y%m%d')
            return df

        self._update(download_data)
        return 0
