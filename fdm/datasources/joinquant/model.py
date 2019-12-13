from time import sleep
from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame


from fdm.datasources.metaclass import (_CollectionBase,
                                       _DbBase,
                                       _DynCollectionBase)

from .feeder import price, FS_temp
from .fields import *

# ----------------------------
# DB Base
# ----------------------------


class JQData(_DbBase):
    def daily_price(self):
        return self._inti_col(DailyPrice)

    def income(self):
        return self._inti_col(Income)

    def cashflow(self):
        return self._inti_col(CashFlow)

    def balance(self):
        return self._inti_col(Balance)

# ----------------------------
# Template
# ----------------------------


class _Template(_DynCollectionBase):
    feeder_func = None
    fields = None

    def update(self, codes,
               startdate,
               enddate,
               force_update=False
               ):
        super().update(codes=codes,
                       fields=self.fields,
                       startdate=startdate,
                       enddate=enddate,
                       force_update=force_update)

    def query(self, codes,
              fields,
              startdate,
              enddate,
              ):
        res = super().query(codes=codes,
                            fields=fields,
                            startdate=startdate,
                            enddate=enddate,
                            skip_update=True)
        return res


class DailyPrice(_Template):
    feeder_func = price
    fields = price_fields


class Income(_Template):
    feeder_func = FS_temp('STK_INCOME_STATEMENT', 11)
    fields = fs_fields['STK_INCOME_STATEMENT']


class CashFlow(_Template):
    feeder_func = FS_temp('STK_CASHFLOW_STATEMENT', 11)
    fields = fs_fields['STK_CASHFLOW_STATEMENT']


class Balance(_Template):
    feeder_func = FS_temp('STK_BALANCE_SHEET', 11)
    fields = fs_fields['STK_BALANCE_SHEET']
