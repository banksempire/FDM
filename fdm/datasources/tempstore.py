import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient

from .metaclass import _DbBase, ColInterface


class TempDB(_DbBase):
    '''Database that holds temporary data.'''

    def __init__(self, client: MongoClient, settingname='tempdb'):
        super().__init__(client, settingname)

    def __getitem__(self, key: str):
        col = self.db[key]
        return ColInterface(col)
