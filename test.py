import fdm


edb = fdm.Wind().edb()
# edb.interface.drop()
data = edb.query(['M5567876', 'M5567877'], autoupdate=False)
print(data.pivot('date', 'code', 'value'))
