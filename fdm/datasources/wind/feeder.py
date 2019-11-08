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


def wsd(cls, code: str, field: str, start: datetime, end: datetime) -> DataFrame:
    # Init wind api
    from WindPy import w
    w.start()
    # unpack params
    try:
        qfield, qparam = field.split('||')
    except:
        qfield = field
        qparam = ''

    data = w.wsd(code, qfield, start, end, qparam)

    if data.ErrorCode == 0:
        # Download data
        df = DataFrame(data.Data, columns=data.Times, index=data.Codes).T
        # Transform data
        df = df.unstack().reset_index()
        df.columns = ['code', 'date', field]
        df['date'] = pd.to_datetime(df['date'])
        return df[(df['date'] <= end) & (df['date'] >= start)]
    elif data.ErrorCode == -40520007:
        return DataFrame()
    else:
        print(data)
        raise ValueError('Error code:{0}'.format(data.ErrorCode))


def wset_sector_constituent(sector_type: str):
    def main(cls, code: str, field: str, start: datetime, end: datetime) -> DataFrame:
        # Init wind api
        from WindPy import w
        w.start()

        assert start == end
        param = "date={d};{t}={c}".format(
            d=start.strftime('%Y-%m-%d'), c=code, t=sector_type)
        data = w.wset("sectorconstituent", param)

        if data.ErrorCode == 0:
            # Download data
            df = DataFrame(data.Data, columns=data.Codes, index=data.Fields).T
            if not df.empty:
                value = df.to_json()
                doc = {
                    'code': code,
                    'date': start,
                    field: value
                }
                return DataFrame([doc])
            else:
                return DataFrame()
        elif data.ErrorCode == -40520007:
            return DataFrame()
        else:
            print(data)
            raise ValueError('Error code:{0}'.format(data.ErrorCode))

    return main
