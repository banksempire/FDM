from datetime import timedelta
from datetime import datetime

import pandas as pd

from fdm.datasources.metaclass import _CollectionBase, _DbBase
from .feeder import (edb)


class Wind(_DbBase):
    def edb(self):
        return self._inti_col(EDB)


class EDB(_CollectionBase):
    def update(self, codes=None, enddate: datetime = datetime.now()):

        if codes is None:
            wind_codes = self.interface.distinct(self.interface.code_name)
        elif isinstance(codes, str):
            wind_codes = [codes]
        else:
            wind_codes = codes

        for code, startdate in ((c, self.interface.lastdate_by_code(c)+timedelta(1))
                                for c in wind_codes):
            df = edb(code, startdate, enddate)
            print(df)
            self.interface.insert_many(df)

    def query(self, code_list_or_str=None,
              date=None,
              startdate: datetime = None,
              enddate: datetime = None,
              freq='D',
              fields: list = None,
              fillna=None,
              autoupdate=True):

        if autoupdate:
            end = datetime.now() if enddate is None else enddate
            self.update(code_list_or_str, end)
        res = self.interface.query(code_list_or_str,
                                   date,
                                   startdate,
                                   enddate,
                                   freq,
                                   fields,
                                   fillna)
        return res
