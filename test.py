from datetime import datetime

from fdm.datasources.metaclass.manager import Manager
from fdm.utils.client import client

from fdm.utils.test import test_feeder_func

'''tdb = client['test']['test']
fm = Manager(tdb)

for i in fm.solve_update_params('abc', 'cde', datetime(2000, 1, 1), datetime(2012, 1, 1)):
    print(i)
client.close()'''

print(test_feeder_func('abc', 'cdb', datetime(2019, 1, 1), datetime(2019, 2, 1)))
