from time import sleep
from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame


import tushare as ts

from fdm.datasources.metaclass import _CollectionBase, _DbBase
from .feeders import rebuilder, updater


class _TushareCollectionBase(_CollectionBase):
    method_name = 'blank'

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

    def rebuild(self, buildindex=False):
        self._rebuild(rebuilder(self.method_name))
        if buildindex:
            self.interface.create_indexs(
                [self.interface.date_name, self.interface.code_name])
        return 0

    def update(self):
        self._update(updater(self.method_name))
        return 0


class Tushare(_DbBase):

    def daily_price(self):
        return self._inti_col(DailyPrice)

    def daily_basic(self):
        return self._inti_col(DailyBasic)

    def daily_adj(self):
        return self._inti_col(DailyAdjFactor)


class DailyBasic(_TushareCollectionBase):
    method_name = 'daily_basic'

    def rebuild(self, buildindex=True):
        return super().rebuild(buildindex)


class DailyPrice(_TushareCollectionBase):
    method_name = 'daily'


class DailyAdjFactor(_TushareCollectionBase):
    method_name = 'adj_factor'
