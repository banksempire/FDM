def Tushare():
    from .tushare.model import Tushare
    return Tushare()


def CleanData():
    from .cleandata.model import CleanData
    return CleanData()


def Wind():
    from .wind.model import Wind
    return Wind()


def TempDB():
    from .tempdb import TempDB
    return TempDB()
