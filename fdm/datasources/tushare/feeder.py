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
