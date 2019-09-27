from fdm.datasources.metaclass.fieldstore import FieldStatus
from fdm.utils.client import client

tdb = client['test']['test']
fs = FieldStatus(tdb)

'''fs['abc', 'asdf2'] = 'test'
print(fs['abc', 'asdf2'])'''

for i in fs:
    print(i)

client.close()
