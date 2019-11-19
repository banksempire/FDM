from datetime import datetime

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


def test_wind():
    codes = '000001.SZ,000002.SZ,000004.SZ,000005.SZ,000006.SZ,000007.SZ,000008.SZ,000009.SZ,000010.SZ,000011.SZ'.split(
        ',')
    wsd = fdm.Wind().wsd()
    time = datetime.now()
    print(wsd.query(codes, fields='close', startdate=datetime(
        2018, 1, 1), enddate=datetime(2019, 4, 1)))
    print(datetime.now()-time)


multi_dummy()
client.close()
