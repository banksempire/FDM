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

    def fin_income(self):
        return self._inti_col(FinIncome)

    def fin_cashflow(self):
        return self._inti_col(FinCashFlow)

    def fin_balance(self):
        return self._inti_col(FinBalance)

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

# ----------------------------
# Financial Statements
# ----------------------------


class Income(_Template):
    feeder_func = FS_temp('STK_INCOME_STATEMENT', 11)
    fields = fs_fields['STK_INCOME_STATEMENT']


class CashFlow(_Template):
    feeder_func = FS_temp('STK_CASHFLOW_STATEMENT', 11)
    fields = fs_fields['STK_CASHFLOW_STATEMENT']


class Balance(_Template):
    feeder_func = FS_temp('STK_BALANCE_SHEET', 11)
    fields = fs_fields['STK_BALANCE_SHEET']


class FinIncome(_Template):
    feeder_func = FS_temp('FINANCE_INCOME_STATEMENT', 11)
    fields = fs_fields['FINANCE_INCOME_STATEMENT']


class FinCashFlow(_Template):
    feeder_func = FS_temp('FINANCE_CASHFLOW_STATEMENT', 11)
    fields = fs_fields['FINANCE_CASHFLOW_STATEMENT']


class FinBalance(_Template):
    feeder_func = FS_temp('FINANCE_BALANCE_SHEET', 11)
    fields = fs_fields['FINANCE_BALANCE_SHEET']
