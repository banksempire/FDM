from datetime import datetime
from collections import defaultdict

from pandas import DataFrame
import pandas as pd

from fdm.utils.exceptions import FeederFunctionError

# ------------------------------
# Feeder Template
# ------------------------------


def feeder_factory(downloader, transformer):
    def generic_func(cls, code: str, field: str, start: datetime, end: datetime) -> DataFrame:
        # Init wind api
        from WindPy import w
        w.start()
        # unpack params
        data = downloader(w, field, code, start, end)

        if data.ErrorCode == 0:
            # Transform data
            return transformer(data, field, end, start, code)
        elif data.ErrorCode == -40520007:
            return DataFrame()
        else:
            print(data)
            raise FeederFunctionError('Error code:{0}'.format(data.ErrorCode))
    return generic_func


# ------------------------------
# Feeders
# ------------------------------

def wsd():
    def wsd_downloader(w, field, code, start, end):
        # unpack params
        try:
            qfield, qparam = field.split('||')
        except:
            qfield = field
            qparam = ''
        # Download data
        data = w.wsd(code, qfield, start, end, qparam)
        return data

    def wsd_transform(data, field, end, start, code):
        # Transform data
        df = DataFrame(data.Data, columns=data.Times, index=data.Codes).T
        df = df.unstack().reset_index()
        df.columns = ['code', 'date', field]
        df['date'] = pd.to_datetime(df['date'])
        return df[(df['date'] <= end) & (df['date'] >= start)]

    return feeder_factory(wsd_downloader, wsd_transform)


def edb():
    def downloader(w, field, code, start, end):
        # Unpack params
        startdate = start.strftime('%Y-%m-%d')\
            if isinstance(start, datetime) else start
        enddate = end.strftime('%Y-%m-%d')\
            if isinstance(end, datetime) else end
        # Download data
        data = w.edb(code, startdate, enddate,
                     "Fill=Previous")
        return data

    def transformer(data, field, end, start, code):
        # Transform data
        df = DataFrame(data.Data, columns=data.Times, index=data.Codes).T
        # Transform data
        df = df.unstack().reset_index()
        df.columns = ['code', 'date', field]
        df['date'] = pd.to_datetime(df['date'])
        return df[(df['date'] <= end) & (df['date'] >= start)]

    return feeder_factory(downloader, transformer)


def wset_sector_constituent(sector_type: str):
    def downloader(w, field, code, start, end):
        # Unpack params
        assert start == end
        param = "date={d};{t}={c}".format(
            d=start.strftime('%Y-%m-%d'), c=code, t=sector_type)
        data = w.wset("sectorconstituent", param)
        return data

    def transformer(data, field, end, start, code):
        # Transform data
        df = DataFrame(data.Data, columns=data.Codes, index=data.Fields).T
        if not df.empty:
            value = df.to_json()
            doc = {
                'code': code,
                'date': start,
                field: value
            }
            return DataFrame([doc])

    return feeder_factory(downloader, transformer)


def wset_index_change():
    def downloader(w, field, code, start, end):
        # Unpack params
        startdate = start.strftime('%Y-%m-%d')\
            if isinstance(start, datetime) else start
        enddate = end.strftime('%Y-%m-%d')\
            if isinstance(end, datetime) else end

        param = "startdate={s};enddate={e};windcode={c};field=tradedate,tradecode,tradestatus".format(
            s=startdate, e=enddate, c=code)
        # Download Data
        data = w.wset("indexhistory", param)
        return data

    def transformer(data, field, end, start, code):
        def gen_res(code, field, res):
            for k, v in res.items():
                res = {
                    'date': k.to_pydatetime(),
                    'code': code,
                    field: ','.join(v)
                }
                yield pd.Series(res)

        # Transform data
        df = DataFrame(data.Data, columns=data.Codes, index=data.Fields).T
        res = defaultdict(list)
        for _, v in df.iterrows():
            res[v['tradedate']].append(
                '|'.join((v['tradecode'], v['tradestatus'])))

        return DataFrame(gen_res(code, field, res))

    return feeder_factory(downloader, transformer)
