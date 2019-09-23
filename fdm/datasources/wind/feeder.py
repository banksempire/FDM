from datetime import datetime

from pandas import DataFrame
import pandas as pd


def edb(codes, start, end) -> DataFrame:
    '''Get edb data from wind'''
    from WindPy import w
    w.start()
    # Unpack params
    wind_codes = codes if isinstance(codes, str) else ','.join(codes)
    startdate = start.strftime('%Y-%m-%d')\
        if isinstance(start, datetime) else start
    enddate = end.strftime('%Y-%m-%d')\
        if isinstance(end, datetime) else end
    # Download data
    data = w.edb(wind_codes, startdate, enddate,
                 "Fill=Previous")
    if data.ErrorCode != 0:
        print(data)
        raise ValueError('Wind request failed!')
    df = DataFrame(data.Data, columns=data.Times, index=data.Codes).T
    # Transform data
    df = df.unstack().reset_index()
    df.columns = ['code', 'date', 'value']
    df['date'] = pd.to_datetime(df['date'])
    return df[(df['date'] <= end) & (df['date'] >= start)]
