from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient

from .metaclass import _CollectionBase, _DbBase

class TempDB(_DbBase):
    '''Database that holds temporary data.'''
    def __init__(self, client: MongoClient, settingname='tempstore'):
        super().__init__(client, settingname)
    
    def __getitem__(self,key:str):
        col = self.db[key]
        return TempCol(col)

class TempCol(_CollectionBase):
    pass