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


class EDB(_DynCollectionBase):
    feeder_func = edb()

    def query(self, codes,
              startdate,
              enddate,
              force_update=False,
              skip_update=False
              ):

        data = super().query(codes=codes,
                             startdate=startdate,
                             enddate=enddate,
                             fields='DATA',
                             force_update=force_update,
                             skip_update=skip_update
                             )
        return data


class WSD(_DynCollectionBase):
    feeder_func = wsd()

# ----------------------------
# WSET index/sector constituent
# ----------------------------


class _Constituent(_DynCollectionBase):
    def query(self, codes,
              date,
              force_update=False,
              skip_update=False
              ):
        assert isinstance(codes, str)
        data = super().query(codes=codes,
                             startdate=date,
                             enddate=date,
                             fields='constituent',
                             force_update=force_update,
                             skip_update=skip_update
                             )
        if isinstance(data, pd.DataFrame):
            df = pd.read_json(data[codes.upper()][0])
            return df

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
