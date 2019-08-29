from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient

from .metaclass import _CollectionBase, _DbBase
from fdm import Tushare, CleanData


class Factors(_DbBase):
    def __init__(self, client: MongoClient, settingname='factorbase'):
        super().__init__(client, settingname)

    def __getitem__(self, key: str):
        keyring = {
            'size': self.size,
        }
        return keyring[key]()

    def size(self):
        colName = self.setting['DBSetting']['colSetting']['size']
        col = self.db[colName]
        return Size(col)


class Size(_CollectionBase):
    def rebuild(self, source: str = 'tushareCache'):
        def _tushare(startdate: datetime, enddate: datetime,
                     q=0.5, category='mv', weight='ew', rpw_days=None):
            ''' params:
            q: percentile to split the portfolio.
                q = 0.3 -> top 30% and bottom 30%
            category: field to split the portfolio
                mv: market value
                fmv: free market value
            weight: weight to construct portfolio
                ew: equal weight
                mw: market value weight
                fmw: free market weight
                lmw: log(market value) weight
                lfmw: log(free market value) weight
                rpw: weight by risk-parity
                drpw: weight by downside risk-parity
            rpw_days: rolling periods to calculate volatility for rpw and drpw
                only applicable when weight = rpw or drpw
            '''
            catcode = {'mv': 'total_mv', 'fmv': 'circ_mv'}

            # Init database
            client = self.get_client()
            tdb = Tushare(client).daily_basic()
            cdb = CleanData(client).pricing()

            # Get category score
            catdata = tdb.query({'trade_date': {'$gt': startdate, '$lte': enddate}},
                                projection=['ts_code', 'trade_date', catcode[category]])
            catdata = catdata.pivot(
                index='trade_date', columns='ts_code', values=catcode[category])
            catdata.sort_index(inplace=True)

            # Split stock codes by category score
            def split_list_by_quantile(ts):
                low = ts[ts.quantile(q) > ts].index
                high = ts[ts.quantile(1-q) <= ts].index
                return pd.Series({'low': list(low), 'high': list(high)})
            split_list = catdata.apply(split_list_by_quantile, axis=1)
            del catdata
            # Get return data
