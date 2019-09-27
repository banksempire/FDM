from fdm.datasources.metaclass.fieldstore import FieldStore
from fdm.utils.client import client

tdb = client['test']['test']
fs = FieldStore(tdb)

for i in 'asdf':
    fs.append(i)
client.close()
