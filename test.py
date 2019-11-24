from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from time import sleep
from random import randint

import fdm
from fdm.utils.client import client
from fdm.utils.test import ord_test_feeder_func, test_config, test_feeder_func_Q

from fdm.datasources.metaclass.interface import StaColInterface
from fdm.datasources.metaclass.manager import FieldStatus


def test_one_dummy():
    client.drop_database('test')
    time = datetime.now()
    interface = StaColInterface(
        client['test']['test'], ord_test_feeder_func, test_config['Test']['DBSetting'])
    print(interface.query(['abc'], fields=['cdb'], startdate=datetime(
        1990, 1, 1), enddate=datetime(2020, 12, 1)))
    print(datetime.now()-time)


def multi_dummy():
    client.drop_database('test')
    time = datetime.now()
    interface = StaColInterface(
        client['test']['test'], ord_test_feeder_func, test_config['Test']['DBSetting'])
    codes = ['code_' + str(i) for i in range(20)]
    print(codes)
    print(interface.query(codes, fields=['cdb'], startdate=datetime(
        1990, 1, 1), enddate=datetime(2019, 4, 1)))
    print(datetime.now()-time)


def test_wind_wsd():
    codes = '000001.SZ,000002.SZ'.split(
        ',')
    '000004.SZ,000005.SZ,000006.SZ,000007.SZ,000008.SZ,000009.SZ,000010.SZ,000011.SZ'
    wsd = fdm.Wind().wsd()
    time = datetime.now()
    print(wsd.query(codes, fields='close||PriceAdj=B', startdate=datetime(
        2019, 3, 15), enddate=datetime(2019, 4, 1), force_update=True))
    print(datetime.now()-time)


def test_wind_edb():
    codes = 'M5567876'
    edb = fdm.Wind().edb()
    time = datetime.now()
    print(edb.query(codes, startdate=datetime(
        2018, 1, 1), enddate=datetime(2019, 4, 1)))
    print(datetime.now()-time)


def test_wind_wset_cons():
    sec = fdm.Wind().sector_constituent()
    date = datetime(2019, 11, 8)
    time = datetime.now()
    print(sec.query('a001010100000000', date))
    print(datetime.now() - time)


def test_tushare_income():
    import tushare as ts
    pro = ts.pro_api()
    codes = list(pro.stock_basic()['ts_code'])
    tus = fdm.Tushare()
    inc = tus.income()
    inc.update(codes,
               datetime(1990, 1, 1),
               datetime(2020, 1, 1))


if __name__ == '__main__':
    fdm.utils.change_client('localhost', 27017)
    print(client)
    test_tushare_income()
    #client['test']['test'].insert_one({'index': 1, 'cde.cde': 'test_value'})
    client.close()
