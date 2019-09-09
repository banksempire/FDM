import json
from datetime import datetime
from typing import Optional

from pymongo.collection import Collection
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd


class ColInterface:
    '''This interface standardized mongodb collection-level operation over
    a series of sub collections.

    Sub collections are splited according to year value of record's timestamp.
    '''

    def __init__(self, col: Collection, setting: dict = None):
        self.col = col
        if setting is None:
            self.code_name = 'code'
            self.date_name = 'name'
        else:
            self.code_name = setting['code_name']
            self.date_name = setting['date_name']

    def list_subcollection_names(self) -> list:
        '''Return all name of all subcollections.'''
        db = self.col.database
        res = []
        for name in db.list_collection_names():
            sname = name.split('.')
            if sname[0] == self.col.name and len(sname) >= 2:
                res.append(sname[1])
        res.sort()
        return res

    def list_subcollections(self):
        '''Return all subcolletions.'''
        subcols = self.list_subcollection_names()
        res = []
        for subcol in subcols:
            res.append(self.col[subcol])
        return res

    def count(self) -> int:
        '''Return number of documents across all sub collections.'''
        subcols = self.list_subcollection_names()
        n = 0
        for subcol in subcols:
            n += self.col[subcol].estimated_document_count()
        return n

    def query(self, code_list_or_str=None, date: datetime = None,
              startdate: datetime = None, enddate: datetime = None,
              fields: list = None):
        '''Qurey records given code(s) and/or date.
        When date is set, startdate and end date will be ignored. '''
        # params preprocessing
        subcol_list = self.list_subcollection_names()

        if date is None:
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
        else:
            startyear = date.year
            startdate = date
            endyear = date.year
            enddate = date

        if code_list_or_str is None:
            qparams: dict = {}
        elif isinstance(code_list_or_str, str):
            qparams = {self.code_name: code_list_or_str}
        elif isinstance(code_list_or_str, list):
            qparams = {self.code_name: {'$in': code_list_or_str}}

        res = DataFrame()

        for year in range(startyear, endyear+1):
            subcol: Collection = self.col[str(year)]
            qstartdate = max(startdate, datetime(year, 1, 1))
            qenddate = min(enddate, datetime(year, 12, 31))
            qparams[self.date_name] = {
                '$gte': qstartdate, '$lte': qenddate}
            cursor = subcol.find(filter=qparams, projection=fields)
            df = DataFrame(cursor)
            res = res.append(df)
        if not res.empty:
            del res['_id']
        return res

    def rolling_query(self, window: str, code_list_or_str=None, date: datetime = None,
                      startdate: datetime = None, enddate: datetime = None,
                      fields: list = None):
        pass

    def insert_many(self, df: DataFrame):
        '''Insert DataFrame into each sub collections accordingly.'''
        date_name = self.date_name
        df[date_name] = pd.to_datetime(df[date_name])
        df = df.sort_values(date_name)
        mindate = min(df[date_name])
        maxdate = max(df[date_name])

        for year in range(mindate.year, maxdate.year+1):
            idf = df[(df[date_name] <= datetime(year, 12, 31)) &
                     (df[date_name] >= datetime(year, 1, 1))]
            record = idf.to_dict('record')
            if len(record) != 0:
                self.col[str(year)].insert_many(record)

        return 0

    def delete_by_date(self, date: datetime):
        '''Delete record given date'''
        col: Collection = self.col[str(date.year)]
        col.delete_many({self.date_name: date})

    def drop(self):
        '''Drop all sub collections.'''
        self.col.drop()
        subcols = self.list_subcollection_names()
        for subcol in subcols:
            self.col[subcol].drop()
        return 0

    def lastdate(self) -> datetime:
        '''Return the max(date) in all sub collections.'''
        subcols = self.list_subcollection_names()
        doc = self.col[subcols[-1]].find(projection=[self.date_name]).sort(
            [(self.date_name, -1)]).limit(1)
        return doc[0][self.date_name]

    def firstdate(self) -> datetime:
        '''Return the min(date) in all sub collections.'''
        subcols = self.list_subcollection_names()
        doc = self.col[subcols[0]].find(projection=[self.date_name]).sort(
            [(self.date_name, 1)]).limit(1)
        return doc[0][self.date_name]

    def full_name(self) -> str:
        '''Return full name of the collection.'''
        return self.col.full_name

    def create_indexs(self, indexes: list):
        '''Create index for all sub collections.'''
        if self.count() != 0:
            for subcol in self.list_subcollections():
                for index in indexes:
                    subcol.create_index(index)
            return 0
        return 1

    def distinct(self, key: str) -> list:
        '''Return list of distinct value key.'''
        res: set = set()
        for subcol in self.list_subcollections():
            res = res.union(subcol.distinct(key))
        return list(res)

    def list_code_names(self):
        '''Return code lists.'''
        res = set()
        for subcol in self.list_subcollections():
            res = res.union(subcol.distinct(self.code_name))
        return res

    def get_client(self) -> MongoClient:
        '''Return MonogClient.'''
        return self.col.database.client


class _CollectionBase:
    '''A simple warper class of ColInterface'''

    def __init__(self, col: Collection, setting: dict):
        self.interface = ColInterface(col, setting)

    def last_record_date(self) -> Optional[datetime]:
        return self.interface.lastdate()

    def query(self, code_list_or_str=None, date: datetime = None,
              startdate: datetime = None, enddate: datetime = None,
              fields: list = None) -> DataFrame:
        df = self.interface.query(code_list_or_str, date,
                                  startdate, enddate,
                                  fields)
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
