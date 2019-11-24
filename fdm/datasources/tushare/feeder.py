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


def daily(cls, code: str, field: str, start: datetime, end: datetime):
    '''
    Tushare daily trading price info.
    '''
    @retry(10)
    def downloader(code, start, end):
        import tushare as ts
        pro = ts.pro_api()
        df = pro.daily(ts_code=code,
                       start_date=start.strftime(
                           '%Y%m%d'),
                       end_date=end.strftime('%Y%m%d'))
        return df

    cache_name = 'daily'
    if tushare_cache[cache_name, code].empty:
        tushare_cache[cache_name, code] = downloader(
            code, start, end)

    data = tushare_cache[cache_name, code]
    res = data[['ts_code',
                'trade_date',
                field]].copy()

    del data[field]
    # delete from cache if all data has been returned
    if data.shape[1] == 2:
        del tushare_cache[cache_name, code]
    return res


def adj_factor(cls, code: str, field: str, start: datetime, end: datetime):
    '''
    Tushare price adjust factor.
    '''
    @retry(10)
    def downloader(code, start, end):
        import tushare as ts
        pro = ts.pro_api()
        df = pro.adj_factor(ts_code=code,
                            start_date=start.strftime(
                                '%Y%m%d'),
                            end_date=end.strftime('%Y%m%d'))
        return df

    cache_name = 'adj_factor'
    if tushare_cache[cache_name, code].empty:
        tushare_cache[cache_name, code] = downloader(
            code, start, end)

    data = tushare_cache[cache_name, code]
    res = data[['ts_code',
                'trade_date',
                field]].copy()

    del data[field]
    # delete from cache if all data has been returned
    if data.shape[1] == 2:
        del tushare_cache[cache_name, code]
    return res


def daily_basic(cls, code: str, field: str, start: datetime, end: datetime):
    '''
    Tushare other daily trading info.
    '''
    @retry(10)
    def downloader(code, start, end):
        import tushare as ts
        pro = ts.pro_api()
        df = pro.daily_basic(ts_code=code,
                             start_date=start.strftime(
                                 '%Y%m%d'),
                             end_date=end.strftime('%Y%m%d'))
        return df

    cache_name = 'daily_basic'
    if tushare_cache[cache_name, code].empty:
        tushare_cache[cache_name, code] = downloader(
            code, start, end)

    data = tushare_cache[cache_name, code]
    res = data[['ts_code',
                'trade_date',
                field]].copy()

    del data[field]
    # delete from cache if all data has been returned
    if data.shape[1] == 2:
        del tushare_cache[cache_name, code]
    return res
