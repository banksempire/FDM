from datetime import timedelta
from datetime import datetime

import pandas as pd

from fdm.datasources.metaclass import (_CollectionBase,
                                       _DbBase,
                                       _DynCollectionBase)
from .feeder import (edb, wsd, wset_sector_constituent)


class Wind(_DbBase):
    def edb(self):
        return self._inti_col(EDB)

    def wsd(self):
        return self._inti_col(WSD)

    def sector_constituent(self):
        return self._inti_col(SectorConstituent)

    def index_constituent(self):
        return self._inti_col(IndexConstituent)


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


class WSD(_DynCollectionBase):
    feeder_func = wsd

# ----------------------------
# WSET index/sector constituent
# ----------------------------


class _Constituent(_DynCollectionBase):
    def query(self, codes,
              fields,
              startdate,
              enddate,
              force_update=False
              ):

        data = super().query(codes=codes,
                             startdate=startdate,
                             enddate=enddate,
                             fields='constituent',
                             force_update=force_update
                             )
        if not data.empty:
            df = pd.read_json(data['constituent'][0])
            return df
        else:
            return data

    def update(self, codes,
               fields,
               startdate,
               enddate,
               force_update=False
               ):

        super().update(codes=codes,
                       startdate=startdate,
                       enddate=enddate,
                       fields='constituent',
                       force_update=force_update
                       )


class SectorConstituent(_Constituent):
    feeder_func = wset_sector_constituent('sectorid')


class IndexConstituent(_Constituent):
    feeder_func = wset_sector_constituent('windcode')
