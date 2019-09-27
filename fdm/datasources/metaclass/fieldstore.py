from datetime import datetime, timedelta

from pymongo.collection import Collection

from fdm.utils.data_structure import Bubbles


class FieldManager():
    def __init__(self, col: Collection):
        self.store = col['FieldStore']
        self.status = col['FieldStatus']


class FieldStore():
    '''FieldStore keep track of all fields avaiable in a collection.'''

    def __init__(self, col: Collection):
        self.col: Collection = col['FieldStore']
        self.cache = self.get_fields()
        self.col.create_index('field', unique=True)

    def __contains__(self, item):
        return item in self.cache

    def __iter__(self):
        return iter(self.cache)

    def append(self, field: str):
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
        self.col: Collection = col['FieldStatus']
        self.fields = FieldStore(col)
        self.col.create_index('code', unique=True)

    def __iter__(self):
        for i in self.col.find():
            yield i

    def __getitem__(self, key):
        code, field = key
        r = self.col.find_one({'code': code}, [field])
        if r is None:
            return Bubbles()
        else:
            return Bubbles(r[field])

    def __setitem__(self, key, value: Bubbles):
        code, field = key
        self.fields.append(field)
        if code in self.col.distinct('code'):
            r = self.col.update_one({'code': code},
                                    {'$set': {field: value.to_list()}})
            assert r.acknowledged
        else:
            r = self.col.insert_one({'code': code, field: value.to_list()})
            assert r.acknowledged

    def __delitem__(self, key):
        code, fields = key
        if code in self.col.distinct('code'):
            if isinstance(fields, str):
                fields = [fields]
            del_dict = {}
            for f in fields:
                del_dict[f] = ''

            r = self.col.update_one({'code': code},
                                    {'$unset': del_dict})
            assert r.acknowledged
        else:
            raise KeyError('Code {} not found in database.'.format(code))
