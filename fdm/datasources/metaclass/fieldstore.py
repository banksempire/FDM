from datetime import datetime, timedelta

from pymongo.collection import Collection


class FieldManager():
    def __init__(self, col: Collection):
        self.store = col['FieldStore']
        self.status = col['FieldStatus']


class FieldStore():
    '''FieldStore keep track of all fields avaiable in a collection.'''

    def __init__(self, col: Collection):
        self.col: Collection = col['FieldStore']
        self.cache = self.get_fields()
        self.create_index()

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

    def create_index(self):
        self.col.create_index('field', unique=True)
