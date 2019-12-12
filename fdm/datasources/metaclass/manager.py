from datetime import datetime, timedelta
from collections import defaultdict

from pymongo.collection import Collection

from fdm.utils.data_structure import Bubbles
from fdm.utils.tools import mongodb_name_compliance


class Manager():
    def __init__(self, col: Collection):
        self.status = FieldStatus(col)
        self.log = Logger(col)

    def __setitem__(self, key, value):
        code, field = key
        self.status[code, field] = value

    def solve_update_params(self, codes: list,
                            fields: list,
                            start: datetime,
                            end: datetime):
        '''Solve params for data that need to be downloaded'''
        target_date_range = [start, end + timedelta(1)]
        status = self.status[codes, fields]
        for code in codes:
            for field in fields:
                comp_code = mongodb_name_compliance(code)
                comp_field = mongodb_name_compliance(field)
                bubbles = status[comp_code, comp_field]
                gaps: Bubbles = bubbles.gaps(
                    target_date_range)
                if not gaps.isempty:
                    yield code, field, bubbles, gaps

    def solve_remove_params(self, codes: list,
                            fields: list,
                            start: datetime,
                            end: datetime):
        target_date_range = [start, end + timedelta(1)]
        status = self.status[codes, fields]
        for code in codes:
            for field in fields:
                comp_code = mongodb_name_compliance(code)
                comp_field = mongodb_name_compliance(field)
                bubbles = status[comp_code, comp_field]
                gaps: Bubbles = bubbles.intersect(
                    target_date_range)
                if not gaps.isempty:
                    yield code, field, bubbles, gaps


class FieldStore():
    '''FieldStore keep track of all fields avaiable in a collection.'''

    def __init__(self, col: Collection):
        self.col: Collection = col['__FieldStore']
        self.cache = self.get_fields()
        self.col.create_index('field', unique=True)

    def __contains__(self, item):
        return item in self.cache

    def __iter__(self):
        return iter(self.cache)

    def append(self, fields):
        for field in fields:
            if not field in self:
                r = self.col.insert_one({'field': field})
                assert r.acknowledged
                self.cache.add(field)

    def drop(self, field: str):
        if field in self:
            r = self.col.delete_one({'field': field})
            assert r.acknowledged
            self.cache.remove(field)

    def get_fields(self) -> set:
        return set(self.col.distinct('field'))


class FieldStatus():
    def __init__(self, col: Collection):
        self.col: Collection = col['__FieldStatus']
        self.fields = FieldStore(col)
        self.col.create_index('code', unique=True)

    def __iter__(self):
        for i in self.col.find():
            yield i

    def __getitem__(self, key):
        codes, fields = key
        codes = [codes] if isinstance(codes, str) else codes
        fields = [fields] if isinstance(fields, str) else fields

        codes = mongodb_name_compliance(codes)
        fields = mongodb_name_compliance(fields)
        if isinstance(key[0], str) and isinstance(key[1], str):
            r = self.col.find_one({'code': codes[0]}, fields)
            try:
                return Bubbles(r[fields])
            except (TypeError, KeyError):
                return Bubbles()
        else:
            docs = self.col.find({'code': {'$in': codes}}, fields+['code'])
            # TODO: unpack result
            res = defaultdict(Bubbles)
            for doc in docs:
                q_fields = set(doc.keys()) - {'code', '_id'}
                for f in q_fields:
                    res[doc['code'], f] = Bubbles(doc[f])
            return res

    def __setitem__(self, key, value: Bubbles):
        code, field = key

        code = code.upper().replace('.', '~')
        field = field.upper().replace('.', '~')

        self.fields.append([field])
        if code in self.col.distinct('code'):
            r = self.col.update_one({'code': code},
                                    {'$set': {field: value.to_list()}})
            assert r.acknowledged
        else:
            r = self.col.insert_one({'code': code, field: value.to_list()})
            assert r.acknowledged

    def __delitem__(self, key):
        code, fields = key

        code = code.upper().replace('.', '~')
        fields = fields.upper().replace('.', '~')

        if code in self.col.distinct('code'):
            if isinstance(fields, str):
                fields = [fields]
            del_dict = {k: '' for k in fields}
            r = self.col.update_one({'code': code},
                                    {'$unset': del_dict})
            assert r.acknowledged
        else:
            raise KeyError('Code {} not found in database.'.format(code))


class Logger():
    def __init__(self, col: Collection):
        self.col: Collection = col['__Log']
        self.cache: list = []

    def insert(self, code, field, bubble):
        doc = self._create_doc(code, field, bubble, 'INSERT')
        self.cache.append(doc)

    def remove(self, code, field, bubble):
        doc = self._create_doc(code, field, bubble, 'REMOVE')
        self.cache.append(doc)

    def flush(self):
        '''Commit cached log to database.'''
        if len(self.cache) != 0:
            r = self.col.insert_many(self.cache)
            assert r.acknowledged
            del self.cache
            self.cache = []

    def _create_doc(self, code, field, bubble, op):
        doc = {
            'Timestamp': datetime.now(),
            'Operation': op,
            'Code': code,
            'Field': field,
            'Bubble_start': bubble.min,
            'Bubble_end': bubble.max
        }
        return doc
