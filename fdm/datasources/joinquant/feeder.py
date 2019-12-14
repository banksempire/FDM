from datetime import datetime
from collections import defaultdict

import pandas as pd
from pandas import DataFrame

from fdm.utils.decorators import retry, timeout


# ------------------------
# Data cache
# ------------------------
jq_cache: defaultdict = defaultdict(DataFrame)

# ------------------------
# Feeders
# ------------------------


def price(cls, code: str, field: str, start: datetime, end: datetime):
    @retry(10)
    def downloader(code, start, end):
        from .api import JQDataAPI as jq
        df = jq().get_price_period(
            code,
            start,
            end
        )
        df['code'] = code
        df['date'] = pd.to_datetime(df['date'])
        return df

    # If jq_cache don't have the data then download it
    if jq_cache['get_price',  code, start, end].empty:
        jq_cache['get_price', code, start, end] = downloader(code, start, end)
    # Get result
    data = jq_cache['get_price', code, start, end]
    res = data[['code', 'date', field]].copy()
    # Remove returned data
    del data[field]
    # delete from cache if all data has been returned
    # only [code, date] left in the sheet
    if data.shape[1] == 2:
        del jq_cache['get_price', code, start, end]
    return res

# ---------Financial Statement----------


def FS_temp(method_name, min_count):
    def func(cls, code: str, field: str, start: datetime, end: datetime):
        @retry(10)
        def downloader(code, table, start, end):
            from .api import JQDataAPI as jq
            df = jq().get_financial_statement(
                code,
                table,
                start,
                end
            )
            for date_type in ('report_date', 'pub_date', 'start_date', 'end_date'):
                df[date_type] = pd.to_datetime(df[date_type])
            return df.rename(columns={'report_date': 'date'})

        # If jq_cache don't have the data then download it
        if jq_cache['FS', method_name, code, start, end].empty:
            jq_cache['FS', method_name, code, start, end] = downloader(
                code, method_name, start, end)
        # Get result
        data = jq_cache['FS', method_name, code, start, end]
        res = data[['code', 'date', field]].copy()
        # Remove returned data
        del data[field]
        # delete from cache if all data has been returned
        # only useless info left in the sheet
        if data.shape[1] == min_count:
            del jq_cache['FS', method_name, code, start, end]

        return res
    return func

# -----------sector constituents-----------


def constituents(cls, code: str, field: str, start: datetime, end: datetime) -> DataFrame:
    from .api import JQDataAPI as jq

    def get_data():
        dates = pd.date_range(start, end, freq='B')
        for date in (d.to_pydatetime() for d in dates):
            v = jq().get_industry_stocks(code, date)
            res = {
                'code': code,
                'date': date,
                field: ',,'.join(v)
            }
            yield res

    return DataFrame(get_data())
