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
    @retry(10)
    def downloader(code, start, end):
        import tushare as ts
        pro = ts.pro_api()
        df = pro.daily(ts_code=code,
                       start_date=start.strftime(
                           '%Y%m%d'),
                       end_date=end.strftime('%Y%m%d'))
        return df

    if tushare_cache['daily', code].empty:
        tushare_cache['daily', code] = downloader(
            code, start, end)

    data = tushare_cache['daily', code]
    res = data[['ts_code',
                'trade_date',
                field]].copy()

    del data[field]
    # delete from cache if all data has been returned
    if data.shape[1] == 2:
        del tushare_cache['daily', code]
    return res
