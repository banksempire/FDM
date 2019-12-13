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
    codes = '000002.SZ,000004.SZ'.split(
        ',')
    ',000005.SZ,000006.SZ,000007.SZ,000008.SZ,000009.SZ,000010.SZ,000011.SZ'
    wsd = fdm.Wind().wsd()
    time = datetime.now()
    print(wsd.query(codes, fields='acctandnotes_rcv||unit=1;rptType=1;Period=Q;Days=Alldays', startdate=datetime(
        2010, 1, 1), enddate=datetime(2019, 4, 1)))  # , force_update=True
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
    date = datetime(2018, 11, 8)
    time = datetime.now()
    print(sec.query('a001010100000000', date))
    print(datetime.now() - time)


def test_wind_wset_index_cons():
    sec = fdm.Wind().index_constituent()
    date = datetime(2005, 12, 20)
    time = datetime.now()
    print(sec.query('000300.SH', date))
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


def test_wind_wset_index_history():
    sec = fdm.Wind().index_history()
    time = datetime.now()
    print(sec.query('000300.SH', startdate=datetime(
        2018, 1, 1), enddate=datetime(2019, 4, 1)))
    print(datetime.now() - time)


def test_jqdata_price():
    price = fdm.JQData().daily_price()
    import jqdatasdk as jd
    jd.auth('user', 'pass')  # 'user', 'pass'

    price.update('000001.XSHE',
                 datetime(2019, 1, 1),
                 datetime(2020, 1, 1), force_update=True)
    df = price.query('000001.XSHE',
                     'open',
                     datetime(1990, 1, 1),
                     datetime(2020, 1, 1))
    print(df)


def test_jqdata_income():
    price = fdm.JQData().income()
    import jqdatasdk as jd
    jd.auth('13794496547', '59LRfBSyDa4KbDPBWDNe')  # 'user', 'pass'

    price.update('000002.XSHE',
                 datetime(2018, 1, 1),
                 datetime(2020, 1, 1), force_update=True)
    df = price.query('000002.XSHE',
                     'total_operating_revenue',
                     datetime(2018, 1, 1),
                     datetime(2020, 1, 1))
    print(df)


def test_jqdata_sector():
    price = fdm.JQData().constituents()
    import jqdatasdk as jd
    jd.auth('user', 'pass')  # 'user', 'pass'

    price.update('801150',
                 datetime(2018, 1, 1),
                 datetime(2018, 2, 1), force_update=True)
    df = price.query('801150',
                     datetime(2018, 1, 1),
                     datetime(2018, 2, 1))
    print(df)


if __name__ == '__main__':
    '''fdm.utils.change_client('localhost', 27017)
    print(client)
    test_tushare_income() '''
    test_jqdata_income()

    #client['test']['test'].insert_one({'index': 1, 'cde.cde': 'test_value'})
    client.close()
