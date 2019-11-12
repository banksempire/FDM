from datetime import datetime

import fdm
from fdm.utils.client import client
from fdm.utils.test import ord_test_feeder_func, test_config, test_feeder_func_Q

from fdm.datasources.metaclass.interface import DynColInterface
from fdm.datasources.metaclass.manager import FieldStatus


'''client.drop_database('test')
time = datetime.now()'''
'''interface = DynColInterface(
    client['test']['test'], ord_test_feeder_func, test_config['Test']['DBSetting'])'''

'''interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2019, 4, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 6, 1), enddate=datetime(2019, 10, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2020, 2, 1))

interface.remove(['abc'], fields=['cdb'], startdate=datetime(
    2019, 2, 1), enddate=datetime(2019, 4, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2020, 12, 1))

print(interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate = datetime(2020, 12, 1))) '''

'''codes = ['code_' + str(i) for i in range(20)]
print(codes)
print(interface.query(codes, fields=['cdb'], startdate=datetime(
    2018, 1, 1), enddate = datetime(2019, 4, 1))) '''
codes = '000001.SZ,000002.SZ,000004.SZ,000005.SZ,000006.SZ,000007.SZ,000008.SZ,000009.SZ,000010.SZ,000011.SZ'.split(
    ',')
wsd = fdm.Wind().wsd()
time = datetime.now()
print(wsd.query(codes, fields='close', startdate=datetime(
    2018, 1, 1), enddate=datetime(2019, 4, 1)))

print(datetime.now()-time)
client.close()
