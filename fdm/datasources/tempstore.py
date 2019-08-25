from datetime import timedelta
from datetime import datetime

import pandas as pd
from pandas import DataFrame

from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection

from .metaclass import _CollectionBase, _DbBase

class TempStore(_DbBase):
    '''Database that holds temporary data.'''
    def __init__(self, client: MongoClient, settingname='tempstore'):
        super().__init__(client, settingname)
    