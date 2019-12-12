from time import sleep
from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame


from fdm.datasources.metaclass import (_CollectionBase,
                                       _DbBase,
                                       _DynCollectionBase)

from .feeder import price
from .fields import *

# ----------------------------
# DB Base
# ----------------------------


class JQData(_DbBase):
    def daily_price(self):
        return self._inti_col(DailyPrice)

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
