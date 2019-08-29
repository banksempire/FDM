import json
from datetime import datetime
from typing import Optional

from pymongo.collection import Collection
from pymongo import MongoClient
from pandas import DataFrame


class ColInterface:
    def __init__(self, col: Collection):
        self.col = col
        self.setting = {'code': 'code', 'date': 'date'}

    def list_subcollection_names(self) -> list:
        db = self.col.database
        res = []
        for name in db.list_collection_names():
            sname = name.split('.')
            if sname[0] == self.col.name and len(sname) >= 2:
                res.append(sname[1])
        res.sort()
        return res

    def count(self) -> int:
        return self.col.estimated_document_count()

    def _query(self, code_list_or_str=None, startdate: datetime = None, enddate: datetime = None,
               fields: list = None):
        # params preprocessing
        subcol_list = self.list_subcollection_names()

        if startdate is None:
            startyear = int(subcol_list[0])
            startdate = datetime(startyear, 1, 1)
        else:
            startyear = startdate.year

        if enddate is None:
            endyear = int(subcol_list[-1])
            enddate = datetime(endyear, 12, 31)
        else:
            endyear = enddate.year

        if code_list_or_str is None:
            qparams: dict = {}
        elif code_list_or_str is str:
            qparams = {self.setting['code']: code_list_or_str}
        elif code_list_or_str is list:
            qparams = {self.setting['code']: {'$in': code_list_or_str}}

        res = DataFrame()

        for year in range(startyear, endyear+1):
            subcol: Collection = self.col[str(year)]
            qstartdate = max(startdate, datetime(year, 1, 1))
            qenddate = min(enddate, datetime(year, 12, 31))
            qparams[self.setting['date']] = {
                '$gte': qstartdate, '$lte': qenddate}
            cursor = subcol.find(filter=qparams, projection=fields)
            res.append(DataFrame(cursor))
        return res

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

    def update(self, filter: dict, update: dict):
        self.col.update_many(filter, update)
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

    def get_client(self) -> MongoClient:
        return self.col.database.client


class _CollectionBase:
    def __init__(self, col: Collection):
        self.col = ColInterface(col)

    def last_record_date(self, datefieldname: str = 'date') -> Optional[datetime]:
        return self.col.lastdate(datefieldname)

    def query(self, filter: dict = None, projection: list = None) -> DataFrame:
        df = self.col.query(filter, projection)
        return df

    def get_client(self) -> MongoClient:
        return self.col.get_client()


class _DbBase:
    def __init__(self, client: MongoClient, settingname: str):
        with open('setting.json', 'r', encoding='utf-8') as file:
            self.setting = json.loads(file.read())[settingname]
        dbName = self.setting['DBSetting']['dbName']
        self.db = client[dbName]

    def __getitem__(self, key) -> _CollectionBase:
        raise NotImplementedError

    def list_collection_names(self) -> list:
        return self.db.list_collection_names()

    def review_setting(self):
        print(self.setting)
        return 0

    def get_client(self) -> MongoClient:
        return self.db.client
