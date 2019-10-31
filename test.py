from datetime import datetime

from fdm.utils.client import client
from fdm.utils.test import test_feeder_func, test_config, test_feeder_func_Q

from fdm.datasources.metaclass.interface import DynColInterface

client.drop_database('test')
time = datetime.now()
interface = DynColInterface(
    client['test']['test'], test_feeder_func_Q, test_config['Test']['DBSetting'])

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2019, 4, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 6, 1), enddate=datetime(2019, 10, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2020, 2, 1))

interface.remove(['abc'], fields=['cdb'], startdate=datetime(
    2019, 2, 1), enddate=datetime(2019, 4, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2020, 12, 1))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2019, 1, 1), enddate=datetime(2020, 12, 1))


print(datetime.now()-time)
client.close()
