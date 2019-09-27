from datetime import datetime
from typing import Optional, Tuple

from pandas import DataFrame

from pymongo.collection import Collection
from pymongo import MongoClient

from fdm.utils import client, config
from .colinterface import ColInterface


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
        colName = self.setting['DBSetting']['colSetting'][class_name]
        col = self.db[colName]
        return colclass(col, self.setting['DBSetting'])

    def get_client(self) -> MongoClient:
        return self.db.client
