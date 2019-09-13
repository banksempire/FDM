from time import sleep
from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient
from pymongo.collection import Collection

import tushare as ts

from fdm.datasources.metaclass import _CollectionBase, _DbBase
from .feeders import rebuilder, updater


class _TushareCollectionBase(_CollectionBase):
    def __init__(self, col: Collection, setting: dict):
        super().__init__(col, setting)

    def _rebuild(self, download_function):
        # Drop all data in collection
        self.interface.drop()
        print('{0} droped'.format(self.interface.full_name()))
        # Inititalize data source
        pro = ts.pro_api()
        # Get stock list
        stock_list = DataFrame()
        for status in "LDP":
            stock_list = stock_list.append(pro.stock_basic(list_status=status))
        # Download data for each stock code
        for _, value in stock_list.iterrows():
            code = value['ts_code']
            record_len = 1
            # minus one day to prevent imcomplete dataset been downloaded
            enddate = datetime.now()-timedelta(1)
            while record_len != 0:
                df = download_function(code, pro, enddate)
                record_len = df.shape[0]
                if record_len != 0:
                    enddate = min(df['trade_date']) - timedelta(1)
                    self.interface.insert_many(df)

            print('Code: {0} downloaded.'.format(code))
            sleep(0.6)
        return 0

    def _update(self, download_function):
        # Inititalize data source
        pro = ts.pro_api()
        # Get last date in DB
        lastdate = self.interface.lastdate()
        # Generate date range business day only
        daterange = pd.date_range(start=lastdate+timedelta(1),
                                  end=datetime.now(), freq="B")
        print('Total {0} data points need to be downloaded.'.format(
            len(daterange)))
        # Download data for each day
        for i in daterange:
            date = i.strftime('%Y%m%d')
            df = download_function(date, pro)
            self.interface.insert_many(df)
            print('Date: {0} downloaded.'.format(date))
            sleep(0.6)
        return 0


class Tushare(_DbBase):
    def __init__(self, client: MongoClient, settingname='tushare'):
        super().__init__(client, settingname)

    def daily_price(self):
        return self._inti_col(DailyPrice, 'daily')

    def daily_basic(self):
        return self._inti_col(DailyBasic, 'dailyBasic')

    def daily_adj(self):
        return self._inti_col(DailyAdjFactor, 'dailyAdjFactor')


class DailyBasic(_TushareCollectionBase):
    method_name = 'daily_basic'

    def rebuild(self):
        self._rebuild(rebuilder(self.method_name))
        self.interface.create_indexs(
            [self.interface.date_name, self.interface.code_name])
        return 0

    def update(self):
        self._update(updater(self.method_name))
        return 0


class DailyPrice(_TushareCollectionBase):
    method_name = 'daily'

    def rebuild(self):
        self._rebuild(rebuilder(self.method_name))
        return 0

    def update(self):
        self._update(updater(self.method_name))
        return 0


class DailyAdjFactor(_TushareCollectionBase):
    method_name = 'adj_factor'

    def rebuild(self):
        self._rebuild(rebuilder(self.method_name))
        return 0

    def update(self):
        self._update(updater(self.method_name))
        return 0