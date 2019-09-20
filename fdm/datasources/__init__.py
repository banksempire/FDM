from .tushare.model import Tushare as _tushare
from .cleandata.model import CleanData as _cleandata
from .wind.model import Wind as _wind
from .tempdb import TempDB as _tempdb


Tushare = _tushare()
CleanData = _cleandata()
Wind = _wind()
TempDB = _tempdb()
