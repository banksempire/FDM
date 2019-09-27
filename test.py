#import fdm
from datetime import datetime
from fdm.utils.data_structure import Bubbles


'''edb = fdm.Wind().edb()
# edb.interface.drop()
data = edb.query(['M5567876', 'M5567877'], autoupdate=False)
print(data.pivot('date', 'code', 'value'))'''


start = datetime.now()
l = [[datetime(2000, 1, 1), datetime(2000, 2, 1)],
     [datetime(2000, 3, 1), datetime(2000, 5, 30)],
     [datetime(2000, 6, 1), datetime(2000, 12, 1)]
     ]

b0 = Bubbles(l)
bs = b0.gaps()

print(b0.to_actualrange())
print(bs.to_actualrange())
