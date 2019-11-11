from datetime import datetime, timedelta
from typing import Optional, Tuple
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

from pandas import DataFrame
import pandas as pd

from pymongo.collection import Collection
from pymongo import MongoClient, UpdateOne

from .manager import Manager
from fdm.utils.data_structure.bubbles import TimeBubble
from fdm.utils.tools import del_id


class ColInterfaceBase():
    def __init__(self, col: Collection, setting: dict = None):
        self.col = col
        if setting is None:
            self.code_name = 'code'
            self.date_name = 'name'
        else:
            self.code_name = setting['code_name']
            self.date_name = setting['date_name']

    # ----------------------------------------
    # Collection info related
    # ----------------------------------------

    def list_subcollection_names(self, ascending: bool = True) -> list:
        '''Return all name of all subcollections.'''
        property_name = ('FieldStatus', 'FieldStore', 'Log')
        db = self.col.database
        res = []
        for name in db.list_collection_names():
            sname = name.split('.')
            if sname[0] == self.col.name and len(sname) >= 2 and sname[1] not in property_name:
                res.append(sname[1])
        res.sort(reverse=not ascending)
        return res

    def list_subcollections(self, ascending: bool = True):
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

    # ----------------------------------------
    # Collection level management
    # ----------------------------------------
    def drop(self):
        '''Drop all sub collections.'''
        self.col.drop()
        subcols = self.list_subcollection_names()
        for subcol in subcols:
            self.col[subcol].drop()
        return 0

    def create_indexs(self, indexes: list = None):
        '''Create index for all sub collections.'''
        indexes = [self.code_name,
                   self.date_name] if indexes is None else indexes
        if self.count() != 0:
            for subcol in self.list_subcollections():
                for index in indexes:
                    subcol.create_index(index)
            return 0
        return 1


class ColInterface(ColInterfaceBase):
    '''This interface standardized mongodb collection-level operation over
    a series of sub collections.

    Sub collections are splited according to year value of record's timestamp.
    '''

    # ----------------------------------------
    # CRUD
    # ----------------------------------------
    def query(self, code_list_or_str=None,
              date=None,
              startdate: Optional[datetime] = None,
              enddate: Optional[datetime] = None,
              freq='B',
              fields: Optional[list] = None,
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
                    for date, value in pdf.iterrows():
                        na_list = _(value)
                        if len(na_list) != 0:
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
            start = pd.date_range(end=date, periods=2, freq=freq)[
                0]+timedelta(1)
            return start.to_pydatetime(), date.to_pydatetime()

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
        if not df.empty:
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

    # ----------------------------------------
    # Qurey date
    # ----------------------------------------

    def lastdate(self) -> datetime:
        '''Return the max(date) in all sub collections.'''
        subcols = self.list_subcollection_names()
        doc = self.col[subcols[-1]].find(projection=[self.date_name]).sort(
            [(self.date_name, -1)]).limit(1)
        return doc[0][self.date_name]

    def lastdate_by_code(self, code, default=datetime(1980, 1, 1)) -> datetime:
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

    # ----------------------------------------
    # Qurey codes
    # ----------------------------------------
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


class DynColInterface(ColInterfaceBase):
    '''ColInterface that deal with dynamic fields.'''

    def __init__(self, col: Collection, feeder_func, setting: dict = None):
        super().__init__(col, setting)
        self.manager = Manager(col)
        self.feeder_func = feeder_func

    def query(self, code_list_or_str,
              fields: list,
              startdate: datetime,
              enddate: datetime,
              force_update=False,
              update_only=False):
        # Convert string code to list
        codes: list = self._convert_codes(code_list_or_str)
        if force_update:
            # Remove targeted data from database if deemed outdated
            self.remove(codes, startdate, enddate, fields)
        self._auto_update(codes, startdate, enddate, fields)
        fields = self._ensure_fields(fields)

        q_doc = {self.code_name: {'$in': codes}}

        if update_only:
            return DataFrame()
        else:
            res = DataFrame()
            for sub_b in TimeBubble(startdate, enddate+timedelta(1)).iter_years():
                q_doc[self.date_name] = sub_b.to_mongodb_date_range()
                year = sub_b.min.year
                subcol = self.col[str(year)]
                res = res.append(DataFrame(subcol.find(q_doc, fields)))

            return del_id(res)

    def remove(self, codes: list,
               startdate: datetime,
               enddate: datetime,
               fields: list,):

        params = self.manager.solve_remove_params(
            codes, fields, startdate, enddate)

        for code, field, bubbles in params:
            status_bubble = self.manager.status[code, field]
            for bubble in bubbles:
                sub_bubble: TimeBubble
                for sub_bubble in bubble.iter_years():
                    filter_doc: dict = {
                        self.code_name: code,
                        self.date_name: {'$gte': sub_bubble.min,
                                         '$lt': sub_bubble.max},
                    }
                    year = sub_bubble.min.year
                    subcol = self.col[str(year)]
                    r = subcol.update_many(filter_doc,
                                           {'$unset': {field: ''}},
                                           upsert=True)
                    assert r.acknowledged
                # Log operation
                status_bubble = status_bubble.carve(bubble)
                self.manager.log.remove(code, field, bubble)
            self.manager.status[code, field] = status_bubble
        self.manager.log.flush()

    def _auto_update(self, codes: list,
                     startdate: datetime,
                     enddate: datetime,
                     fields: list):
        update_params = self.manager.solve_update_params(
            codes, fields, startdate, enddate)
        for code, field, bubbles in update_params:  
            status_bubble = self.manager.status[code, field]
            b_len = len(bubbles)-1
            for i, bubble in enumerate(bubbles):
                # Download data
                start, end = bubble.to_actualrange()
                df: DataFrame = self.feeder_func(code, field, start, end)
                self._insert(df, code, field, bubble)
                # Update Bubbles in FieldStatus
                if df.empty:
                    # To deal with data freq other than B or D
                    # Will fill in gaps even with no data returned, except the last one
                    if i != b_len:
                        status_bubble = status_bubble.merge(bubble)
                else:
                    dates = df[self.date_name]
                    date_s = min(dates).to_pydatetime()
                    date_e = (max(dates) + timedelta(1)).to_pydatetime()
                    # Log operation
                    status_bubble = status_bubble.merge(
                        TimeBubble(date_s, date_e))
                self.manager.log.insert(code, field, bubble)
            self.manager.status[code, field] = status_bubble
        self.manager.log.flush()

    def _convert_codes(self, code_list_or_str) -> list:
        '''Convert codes to deal with multi type.'''
        if isinstance(code_list_or_str, str):
            return [code_list_or_str]
        elif code_list_or_str is None:
            raise KeyError('code_list_or_str cannot be None in dynamic db')
        else:
            return code_list_or_str

    def _ensure_fields(self, fields) -> list:
        '''Ensure fields contain date and code and remove duplicated value.'''
        res = set(fields)
        res = res.union({self.code_name, self.date_name})
        return list(res)

    def _insert(self, df: DataFrame, code, field, bubble):
        '''Insert DataFrame into each sub collections accordingly.'''
        def form_bulk_write(records):
            bulks = defaultdict(list)
            for record in records:
                year = str(record[self.date_name].year)
                q_doc: dict = {
                    self.date_name: record[self.date_name],
                    self.code_name: record[self.code_name],
                }
                bulks[year].append(
                    UpdateOne(q_doc, {'$set': record}, upsert=True))
            return bulks

        if not df.empty:
            date_name = self.date_name
            df[date_name] = pd.to_datetime(df[date_name])
            df = df.sort_values(date_name)

            records = df.to_dict('record')
            bulks = form_bulk_write(records)
            for year, v in bulks.items():
                subcol = self.col[year]
                subcol.bulk_write(v, ordered=False)
