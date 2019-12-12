'''
import jqdatasdk as jd
from jqdatasdk import finance

q=jd.query(finance.STK_INCOME_STATEMENT).filter(
    finance.STK_INCOME_STATEMENT.code=='600519.XSHG',
    finance.STK_INCOME_STATEMENT.pub_date>='2015-01-01',
    finance.STK_INCOME_STATEMENT.report_type==0).limit(20)
df=finance.run_query(q)
df

'''


from datetime import datetime
from collections import defaultdict

import pandas as pd
from pandas import DataFrame

from fdm.utils.decorators import retry


# ------------------------
# Data cache
# ------------------------
jq_cache = defaultdict(DataFrame)

# ------------------------
# Feeders
# ------------------------


def price(cls, code: str, field: str, start: datetime, end: datetime):
    @retry(10)
    def downloader(code, start, end, mode):
        import jqdatasdk as jd
        df = jd.get_price(
            security=code,
            start_date=start,
            end_date=end,
            frequency='daily',
            skip_paused=True,
            fq=mode,
            panel=False,
            fill_paused=False
        )

        if not mode is None:
            df = df.rename(columns={col: '||'.join((col, mode))
                                    for col in df.columns})

        df.index.name = 'date'
        df = df.reset_index()
        df['code'] = code

        return df

    # prepare adjustment mode
    mode = field.split('||')[1] if '||' in field else None
    # If jq_cache don't have the data then download it
    if jq_cache['get_price', mode, code].empty:
        jq_cache['get_price', mode, code] = downloader(
            code, start, end, mode)
    # Get result
    data = jq_cache['get_price', mode, code]
    res = data[['code', 'date', field]].copy()
    # Remove returned data
    del data[field]
    # delete from cache if all data has been returned
    # only [code, date] left in the sheet
    if data.shape[1] == 2:
        del jq_cache['get_price', mode, code]
    return res
