import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient

from .metaclass import _DbBase, ColInterface


class TempDB(_DbBase):
    '''Database that holds temporary data.'''

    def __getitem__(self, key: str):
        col = self.db[key]
        return ColInterface(col, self.setting['DBSetting'])
