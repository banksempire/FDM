import json
from datetime import datetime
from datetime import timedelta
from typing import Optional, Tuple

from pymongo.collection import Collection
from pymongo import MongoClient
from pandas import DataFrame
import pandas as pd
import numpy as np

from fdm.utils import client, config

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
    
    #----------------------------------------
    # Collection info and 
    #----------------------------------------
    def list_subcollection_names(self,ascending:bool=True) -> list:
        '''Return all name of all subcollections.'''
        db = self.col.database
        res = []
        for name in db.list_collection_names():
            sname = name.split('.')
            if sname[0] == self.col.name and len(sname) >= 2 and sname.isnumeric():
                res.append(sname[1])
        res.sort(reverse=not ascending)
        return res

    def list_subcollections(self, ascending:bool=True):
        '''Return all subcolletions.'''
        subcols = self.list_subcollection_names(ascending)
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

    def full_name(self) -> str:
        '''Return full name of the collection.'''
        return self.col.full_name

    def get_client(self) -> MongoClient:
        '''Return MonogClient.'''
        return self.col.database.client

    #----------------------------------------
    # CRUD
    #----------------------------------------
    def query(self, code_list_or_str=None,
              date=None,
              startdate: datetime = None,
              enddate: datetime = None,
              freq='B',
              fields: list = None,
              fillna=None):
        '''Query data from database.

        code_list_or_str: [None, code, List[codes]] when set to None, will query all codes.
        date: [None, datetime, List[datetime]] when set, startdate, enddate, freq will be ignored.
        startdate: [None, datetime]
        enddate: [None, datetime]
        freq: freq type of pandas date_range
        fields: fields ot return from database
        fillna: [None, 'ffill', 'bfill']
        '''
        def gen_code_filter(codes) -> dict:
            '''Generate filter doc base on code or a list of codes'''
            if codes is None:
                return {}
            elif isinstance(codes, str):
                return {self.code_name: codes}
            else:
                return {self.code_name: {'$in': list(codes)}}

        def query_on_dates(codes, dates, fields) -> DataFrame:
            '''Query data on date or a list of dates.'''
            q_params = gen_code_filter(codes)
            if isinstance(dates, (datetime, pd.Timestamp)):
                q_params[self.date_name] = dates
                year = dates.year
                res = DataFrame(self.col[str(year)].find(
                    q_params, projection=fields))
                return res
            else:
                res = DataFrame()
                for year in {i.year for i in dates}:
                    q_dates = [i for i in dates if i.year == year]
                    q_params[self.date_name] = {'$in': q_dates}
                    df = DataFrame(self.col[str(year)].find(
                        q_params, projection=fields))
                    res = res.append(df)
                return res

        def query_on_daterange(codes, start: datetime, end: datetime, fields, freq: str) -> DataFrame:
            '''Query data on given date range and frequency'''
            q_params = gen_code_filter(codes)
            if freq in ('B', 'D'):
                res = DataFrame()
                for year in range(start.year, end.year+1):
                    subcol = self.col[str(year)]
                    q_params[self.date_name] = {
                        '$gte': max(start, datetime(year, 1, 1)),
                        '$lte': min(end, datetime(year, 12, 31))
                    }
                    cursor = subcol.find(q_params, projection=fields)
                    df = DataFrame(cursor)
                    res = res.append(df)
                return res
            else:
                daterange = pd.date_range(
                    start=start, end=end, freq=freq, normalize=True)
                return query_on_dates(codes, daterange, fields)

        def deal_nonetype_start_end(start, end) -> Tuple[datetime, datetime]:
            '''Interprete start and end date if is None'''
            subcols = self.list_subcollection_names()
            if start is None:
                year = int(subcols[0])
                start = datetime(year, 1, 1)
            if end is None:
                year = int(subcols[-1])
                end = datetime(year, 12, 31)
            return start, end

        def ensure_date_code_fields(f) -> Optional[list]:
            '''Ensure [code, date] in query fields.'''
            if not f is None:
                f = f.copy()
                f = list(set(f).union((self.code_name, self.date_name)))
            return f

        def del_id(df) -> DataFrame:
            '''Delete column['_id'] from result'''
            if not df.empty:
                del df['_id']
            return df

        def fill_nan(df: DataFrame, freq, method) -> DataFrame:
            def iter_na_list_by_date():
                if not df.empty:
                    def _(s: pd.Series):
                        isna = s.isna()
                        isna[isna == True]
                        res = isna[isna == True].index
                        return set(res)
                    
                    pdf = df.pivot(index=date_name, columns=code_name,
                                   values=code_name)
                    pdf = pdf.reindex(pd.date_range(
                        startdate, enddate, freq=freq))
                    for date ,value in pdf.iterrows():
                        na_list = _(value)
                        if len(na_list) !=0:
                            yield date, na_list
                else:
                    if code_list_or_str is None:
                        index = pd.date_range(startdate, enddate, freq=freq)
                        for date in index:
                            yield date, None
                    else:
                        index = pd.date_range(startdate, enddate, freq=freq)
                        na_list = set(code_list_or_str)
                        for date in index:
                            yield date, na_list

            def cutoff_methods(key):
                def f_cutoff(date):
                    return pd.date_range(end=date, periods=2, freq=freq)[0]

                def b_cutoff(date):
                    return pd.date_range(start=date, periods=2, freq=freq)[-1]

                keyring = {
                    'ffill': f_cutoff,
                    'bfill': b_cutoff, }
                try:
                    return keyring[key]
                except:
                    raise KeyError('Unexpected fillna method: {0}'.format(key))

            def fill_data(df, date, cutoff, codes):
                if codes is None:
                    codes = self.list_code_names()
                if method == 'ffill':
                    try:
                        near_date = self.get_nearest_date(codes, date, True)
                        if near_date > cutoff:
                            res = query_on_dates(codes, near_date, fields)
                            res[date_name] = date
                            df = df.append(res)
                            filled_codes = set(res[code_name])
                            unfilled_codes = codes - filled_codes
                            if len(unfilled_codes) > 0:
                                df = fill_data(
                                    df, date, cutoff, unfilled_codes)
                    except KeyError:
                        # Solve KeyError induced by self.get_nearest_date if no value return
                        pass
                elif method == 'bfill':
                    try:
                        near_date = self.get_nearest_date(codes, date, False)
                        if near_date < cutoff:
                            res = query_on_dates(codes, near_date, fields)
                            res[date_name] = date
                            df = df.append(res)
                            filled_codes = set(res[code_name])
                            unfilled_codes = codes - filled_codes
                            if len(unfilled_codes) > 0:
                                df = fill_data(
                                    df, date, cutoff, unfilled_codes)
                    except KeyError:
                        pass
                return df

            cutoff_method = cutoff_methods(method)
            date_name = self.date_name
            code_name = self.code_name

            for date, codes in iter_na_list_by_date():
                cutoff = cutoff_method(date)
                df = fill_data(df, date, cutoff, codes)
            return df

        fields = ensure_date_code_fields(fields)
        if not date is None:
            res = query_on_dates(code_list_or_str, date, fields)
            return del_id(res)
        else:
            start, end = deal_nonetype_start_end(startdate, enddate)
            res = query_on_daterange(
                code_list_or_str, start, end, fields, freq)
            if fillna is None or freq in ('B', 'D'):
                return del_id(res)
            else:
                return del_id(fill_nan(res, freq, fillna))

    def rolling_query(self,
                      window: int,
                      code_list_or_str=None,
                      startdate: datetime = None,
                      enddate: datetime = None,
                      fields: list = None,
                      freq='B',
                      ascending=True,
                      fillna='ffill') -> DataFrame:
        '''Get a rolling window from collection.'''

        if startdate is None:
            startdate = self.firstdate()
        if enddate is None:
            enddate = self.lastdate()

        drange = pd.date_range(start=startdate,
                               end=enddate,
                               freq=freq,
                               normalize=True).sort_values(ascending=ascending)

        result = DataFrame()
        records_count = []
        def gen_start_end(date):
            start = pd.date_range(end = date,periods=2,freq = freq)[0]+timedelta(1)
            return start.to_pydatetime(),date.to_pydatetime()
        
        for start, end in (gen_start_end(date) for date in drange):
            df = self.query(code_list_or_str,
                            startdate=start,
                            enddate=end,
                            freq=freq,
                            fields=fields,
                            fillna=fillna)

            if not df.empty:
                result = result.append(df)
                records_count.append(df.shape[0])

            if len(records_count) == window:
                yield result
                result = result.iloc[records_count.pop(0):]

    def insert_many(self, df: DataFrame):
        '''Insert DataFrame into each sub collections accordingly.'''
        if not df.empty :
            date_name = self.date_name
            df[date_name] = pd.to_datetime(df[date_name])
            df = df.sort_values(date_name)
            mindate = min(df[date_name])
            maxdate = max(df[date_name])

            for year in range(mindate.year, maxdate.year+1):
                idf = df[(df[date_name]<= datetime(year, 12, 31)) &
                        (df[date_name] >= datetime(year, 1, 1))]
                record = idf.to_dict('record')
                if len(record) != 0:
                    self.col[str(year)].insert_many(record)

            return 0

    def delete_by_date(self, date: datetime):
        '''Delete record given date'''
        col: Collection = self.col[str(date.year)]
        col.delete_many({self.date_name: date})
    
    #----------------------------------------
    # Collection level management
    #----------------------------------------
    def drop(self):
        '''Drop all sub collections.'''
        self.col.drop()
        subcols = self.list_subcollection_names()
        for subcol in subcols:
            self.col[subcol].drop()
        return 0

    def create_indexs(self, indexes: list = None):
        '''Create index for all sub collections.'''
        indexes = [self.code_name,self.date_name] if indexes is None else indexes
        if self.count() != 0:
            for subcol in self.list_subcollections():
                for index in indexes:
                    subcol.create_index(index)
            return 0
        return 1

    #----------------------------------------
    # Qurey date
    #----------------------------------------
    def lastdate(self) -> datetime:
        '''Return the max(date) in all sub collections.'''
        subcols = self.list_subcollection_names()
        doc = self.col[subcols[-1]].find(projection=[self.date_name]).sort(
            [(self.date_name, -1)]).limit(1)
        return doc[0][self.date_name]

    def lastdate_by_code(self, code, default = datetime(1980,1,1)) -> datetime:
        '''Get max(date) of a code, if not found return default.'''
        try:
            return self.get_nearest_date([code])
        except:
            return default

    def get_nearest_date(self, code_list, date=datetime.now(), latest=True) -> datetime:
        '''Return the latest/earliest record given code and date'''
        filter_doc = {self.code_name: {'$in': list(code_list)}}

        if latest:
            filter_doc[self.date_name] = {'$lte': date}
            order = -1
        else:
            filter_doc[self.date_name] = {'$gte': date}
            order = 1
        maxyear = datetime.now().year
        year = date.year
        while year in range(1980, maxyear+1):
            try:
                doc = self.col[str(year)].find(filter_doc, projection=[self.date_name]).sort(
                    [(self.date_name, order)]).limit(1)
                return doc[0][self.date_name]
            except:
                year += order
        raise KeyError(
            'Cannot find codes: {0} in given database'.format(code_list))

    def firstdate(self) -> datetime:
        '''Return the min(date) in all sub collections.'''
        subcols = self.list_subcollection_names()
        doc = self.col[subcols[0]].find(projection=[self.date_name]).sort(
            [(self.date_name, 1)]).limit(1)
        return doc[0][self.date_name]
    
    #----------------------------------------
    # Qurey codes
    #----------------------------------------
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

class DynColInterface(ColInterface):
    '''ColInterface that deal with dynamic fields.'''
    def __init__(self, col: Collection, setting: dict = None):
        super().__init__(col,setting)
        self.fields = self.get_fields()
    
    #----------------------------------------
    # Field info management
    #----------------------------------------
    def reg_new_field(self, field_name:str):
        fs = self.col['FieldStore']
        if not field_name in self.fields:
            fs.insert({'field': field_name})
        self.fields = self.get_fields()
        if len(self.fields) == 1:
            fs.create_index('field')

    def get_fields(self) ->set:
        fs = self.col['FieldStore']
        return set(fs.distinct('field'))

    def get_field_record_date(self, code: str,
                              field:str,
                              default:datetime = datetime(1980,1,1),
                              last=True)->datetime: 
        subcols = self.list_subcollections(not last)
        order = -1 if last else 1
        filter_doc = {
            self.code_name: code,
            field:{'$exists':True}
        }
        for subcol in subcols:
            cursor = subcol.find(filter_doc,[self.date_name])\
                .sort([(self.date_name, order)]).limit(1)
            l = list(cursor)
            if len(l) >0 :
                return l[0][self.date_name]
        return default

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
    def __init__(self, client:MongoClient = client.client):
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
