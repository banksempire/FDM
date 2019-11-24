from datetime import datetime
from collections import defaultdict

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
# Feeder Template
# -------------------------------


def template(func_name, min_columns_count):
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
            return df
        # If tushare_cache don't have the data then download
        if tushare_cache[func_name, code].empty:
            tushare_cache[func_name, code] = downloader(
                code, start, end)
        # Get result
        data = tushare_cache[func_name, code]
        res = data[['ts_code',
                    'trade_date',
                    field]].copy()
        # Remove returned data
        del data[field]
        # delete from cache if all data has been returned
        if data.shape[1] == min_columns_count:
            del tushare_cache[func_name, code]
        return res
    return func


daily = template('daily', 2)

adj_factor = template('adj_factor', 2)

daily_basic = template('daily_basic', 2)
