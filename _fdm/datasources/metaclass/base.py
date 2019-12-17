from datetime import datetime
from typing import Optional, Tuple

from pandas import DataFrame

from pymongo.collection import Collection
from pymongo import MongoClient

from fdm.utils import client, config
from fdm.utils.test import test_feeder_func
from .interface import ColInterface, StaColInterface


class _CollectionBase:
    '''A simple warper class of ColInterface'''

    def __init__(self, col: Collection, setting: dict):
        self.interface = ColInterface(col, setting)

    def last_record_date(self) -> Optional[datetime]:
        return self.interface.lastdate()

    def query(self, code_list_or_str=None, date=None,
              startdate: datetime = None, enddate: datetime = None,
              freq='B', fields: list = None, fillna=None) -> DataFrame:
        df = self.interface.query(code_list_or_str,
                                  date,
                                  startdate,
                                  enddate,
                                  freq,
                                  fields,
                                  fillna)
        return df

    def batch_dump(self, batch_size=2000):
        '''Dump all records to a df. Not work for sub collection'''
        i = 0
        l = list()
        for doc in self.interface.col.find():
            l.append(doc)
            i += 1
            if i == batch_size:
                df = DataFrame(l)
                del df['_id']
                yield df
                l = list()
                i = 0

    def get_client(self) -> MongoClient:
        return self.interface.get_client()


class _DynCollectionBase:
    '''A simple warper class of ColInterface'''
    feeder_func = test_feeder_func

    def __init__(self, col: Collection, setting: dict):
        self.interface = StaColInterface(col, self.feeder_func, setting)

    def query(self, codes,
              fields,
              startdate,
              enddate,
              force_update=False,
              skip_update=False
              ):

        # Prepare params
        codes, fields, startdate, enddate = self.convert_params(
            codes, fields, startdate, enddate)

        # Get data
        res = self.interface.query(codes=codes,
                                   fields=fields,
                                   startdate=startdate,
                                   enddate=enddate,
                                   force_update=force_update,
                                   skip_update=skip_update
                                   )
        if len(fields) == 1:
            return res[fields[0].upper().replace('.', '~')]
        else:
            return res

    def update(self, codes,
               fields,
               startdate,
               enddate,
               force_update=False
               ):

        # Prepare params
        codes, fields, startdate, enddate = self.convert_params(
            codes, fields, startdate, enddate)

        # Get data
        self.interface.query(codes=codes,
                             fields=fields,
                             startdate=startdate,
                             enddate=enddate,
                             force_update=force_update,
                             update_only=True
                             )

    def remove(self, codes,
               fields,
               startdate,
               enddate):
        codes, fields, startdate, enddate = self.convert_params(
            codes, fields, startdate, enddate)

        self.interface.remove(codes, startdate, enddate, fields)

    def convert_params(self, codes, fields, startdate, enddate):
        def convert_dt(df):
            return datetime.strptime(df, '%Y-%m-%d')

        codes = codes if not isinstance(codes, str) else[codes]
        fields = fields if not isinstance(fields, str) else[fields]
        startdate = startdate if not isinstance(
            startdate, str) else convert_dt(startdate)
        enddate = enddate if not isinstance(
            enddate, str) else convert_dt(enddate)
        return codes, fields, startdate, enddate

    def create_index(self):
        self.interface.create_indexs()


class _DbBase:
    def __init__(self, client: MongoClient = client.client):
        class_name = self.__class__.__name__
        self.setting = config[class_name]
        dbName = self.setting['DBSetting']['dbName']
        self.db = client[dbName]

    def __getitem__(self, key):
        s = '{key} cannot be found in object {o}'.format(
            key=key, o=self.__class__.__name__)
        return getattr(self, key, s)()

    def list_collection_names(self) -> list:
        return self.db.list_collection_names()

    def review_setting(self):
        print(self.setting)
        return 0

    def _inti_col(self, colclass):
        class_name = colclass.__name__
        try:
            colName = self.setting['DBSetting']['colSetting'][class_name]
        except:
            colName = colclass.__name__
        col = self.db[colName]
        return colclass(col, self.setting['DBSetting'])

    def get_client(self) -> MongoClient:
        return self.db.client
