from datetime import datetime

from fdm.datasources.metaclass.manager import Manager
from fdm.utils.client import client

tdb = client['test']['test']
fm = Manager(tdb)

for i in fm.solve_update_params('abc', 'cde', datetime(2000, 1, 1), datetime(2012, 1, 1)):
    print(i)
client.close()
