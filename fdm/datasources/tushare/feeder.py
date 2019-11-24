from datetime import datetime
from collections import defaultdict
from time import sleep

import pandas as pd
from pandas import DataFrame

from fdm.utils.decorators import retry


def rebuilder(method, max_retry=10):
    @retry(max_retry)
    def downloader(code, pro, end_date,):
        func = getattr(
            pro,
            method,
            'Tushare do not have method {0}'.format(method))
        df = func(ts_code=code,
                  end_date=end_date.strftime('%Y%m%d'))
        df['trade_date'] = pd.to_datetime(
            df['trade_date'], format='%Y%m%d')
        return df
    return downloader


def updater(method, max_retry=10):
    @retry(max_retry)
    def downloader(date, pro):
        func = getattr(
            pro,
            method,
            'Tushare do not have method {0}'.format(method))
        df = func(trade_date=date)
        df['trade_date'] = pd.to_datetime(
            df['trade_date'], format='%Y%m%d')
        return df
    return downloader


tushare_cache = defaultdict(DataFrame)

# -------------------------------
# Trading Info
# -------------------------------


def trading_temp(func_name, min_columns_count):
    def func(cls, code: str, field: str, start: datetime, end: datetime):
        '''
        Feeder function for tushare {} info.'''.format(func_name)

        @retry(10)
        def downloader(code, start, end):
            import tushare as ts
            pro = ts.pro_api()
            df = getattr(pro, func_name)(ts_code=code,
                                         start_date=start.strftime(
                                             '%Y%m%d'),
                                         end_date=end.strftime('%Y%m%d'))
            sleep(0.2)
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df = df.rename(columns={'trade_date': 'date', 'ts_code': 'code'})
            return df
        # If tushare_cache don't have the data then download
        if tushare_cache[func_name, code].empty:
            tushare_cache[func_name, code] = downloader(
                code, start, end)
        # Get result
        data = tushare_cache[func_name, code]
        res = data[['code', 'date', field]].copy()
        # Remove returned data
        del data[field]
        # delete from cache if all data has been returned
        if data.shape[1] == min_columns_count:
            del tushare_cache[func_name, code]
        return res
    return func


# Daily pricing data
daily = trading_temp('daily', 2)

# Daily adjust factor
adj_factor = trading_temp('adj_factor', 2)

# Daily trading info
daily_basic = trading_temp('daily_basic', 2)


# -------------------------------
# Financial Statement Info
# -------------------------------


def fs_temp(func_name, min_columns_count):
    def func(cls, code: str, field: str, start: datetime, end: datetime):
        '''
        Feeder function for tushare {} info.'''.format(func_name)

        @retry(10)
        def downloader(code, start, end):
            import tushare as ts
            pro = ts.pro_api()
            df = getattr(pro, func_name)(ts_code=code,
                                         start_date=start.strftime(
                                             '%Y%m%d'),
                                         end_date=end.strftime('%Y%m%d'))
            sleep(0.2)
            columns = ('ann_date', 'f_ann_date', 'end_date')
            for column in columns:
                try:
                    df[column] = pd.to_datetime(df[column])
                except:
                    pass
            df = df.rename(columns={'end_date': 'trade_date'})
            return df
        # If tushare_cache don't have the data then download
        if tushare_cache[func_name, code].empty:
            tushare_cache[func_name, code] = downloader(
                code, start, end)
        # Get result
        data = tushare_cache[func_name, code]
        res = data[['code', 'date', field]].copy()
        # Remove returned data
        del data[field]
        # delete from cache if all data has been returned
        if data.shape[1] == min_columns_count:
            del tushare_cache[func_name, code]
        return res
    return func


# Income Statement
income = fs_temp('income', 2)

# Balance Sheet
balancesheet = fs_temp('balancesheet', 2)

# Cash Flow Statement
cashflow = fs_temp('cashflow', 2)

# Performence Forecast
forecast = fs_temp('forecast', 2)

# Performence Express
express = fs_temp('express', 2)

# Financial Matrix
fina_indicator = fs_temp('fina_indicator', 2)
