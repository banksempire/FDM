import json
from datetime import datetime
from typing import Optional

from pymongo.collection import Collection
from pymongo import MongoClient
from pandas import DataFrame


class ColInterface:
    def __init__(self, col: Collection):
        self.col = col

    def count(self) -> int:
        return self.col.estimated_document_count()

    def query(self, filter: dict = None, projection: list = None) -> DataFrame:
        df = DataFrame(self.col.find(filter, projection))
        if not df.empty:
            del df['_id']
        return df

    def insert_many(self, df: DataFrame):
        if df.empty:
            print('Empty Dataframe, insert_many abort.')
            return 1
        record = df.to_dict('record')
        self.col.insert_many(record)
        print('{0} records inserted into {1}'.format(
            len(record), self.full_name()))
        return 0

    def update_on_datecode(self, df: DataFrame, date_name: str = 'date',
                           code_name: str = 'code'):
        for _, v in df.iterrows():
            code = v[code_name]
            date = v[date_name]
            self.col.update_one(filter={'date': date, 'code': code}, update={'$set':
                                                                             v.to_dict()})
        return 0

    def drop(self):
        self.col.drop()
        return 0

    def lastdate(self, datefieldname: str = 'date') -> Optional[datetime]:
        if self.count() != 0:
            doc = self.col.find(projection=[datefieldname]).sort(
                [(datefieldname, -1)]).limit(1)
            return doc[0][datefieldname]
        else:
            return None

    def full_name(self) -> str:
        return self.col.full_name

    def create_indexs(self, indexes: list):
        if self.count() != 0:
            for index in indexes:
                self.col.create_index(index)
            return 0
        return 1

    def distinct(self, key: str) -> list:
        return self.col.distinct(key)


class _CollectionBase:
    def __init__(self, col: Collection):
        self.col = ColInterface(col)

    def last_record_date(self, datefieldname: str = 'date') -> Optional[datetime]:
        return self.col.lastdate(datefieldname)

    def query(self, filter: dict = None, projection: list = None) -> DataFrame:
        df = self.col.query(filter, projection)
        return df


class _DbBase:
    def __init__(self, client: MongoClient, settingname: str):
        with open('setting.json', 'r', encoding='utf-8') as file:
            self.setting = json.loads(file.read())[settingname]
        dbName = self.setting['DBSetting']['dbName']
        self.db = client[dbName]

    def __getitem__(self, key) -> _CollectionBase:
        raise NotImplementedError

    def list_collections(self) -> list:
        return self.db.list_collections()

    def review_setting(self):
        print(self.setting)
        return 0
