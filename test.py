from datetime import datetime

from fdm.utils.client import client
from fdm.utils.test import test_feeder_func, test_config

from fdm.datasources.metaclass.interface import DynColInterface

time = datetime.now()
interface = DynColInterface(
    client['test']['test'], test_feeder_func, test_config['Test']['DBSetting'])

interface.query('abc', fields=['cdb'], startdate=datetime(
    2000, 1, 1), enddate=datetime(2005, 12, 31))

interface.query('abc', fields=['cdb'], startdate=datetime(
    2008, 1, 1), enddate=datetime(2009, 12, 31))

interface.query('abc', fields=['cdb'], startdate=datetime(
    1998, 1, 1), enddate=datetime(2019, 12, 31))

print(datetime.now()-time)
client.close()
