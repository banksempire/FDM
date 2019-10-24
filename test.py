from datetime import datetime

from fdm.utils.client import client
from fdm.utils.test import test_feeder_func, test_config

from fdm.datasources.metaclass.interface import DynColInterface

interface = DynColInterface(
    client['test']['test'], test_feeder_func, test_config['Test']['DBSetting'])
print(interface.full_name())

client.close()
