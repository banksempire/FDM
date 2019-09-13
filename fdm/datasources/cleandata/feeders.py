from pandas import DataFrame

import fdm

feeder_funcs = {
    'tushare': _tushare
}


def _tushare(startdate, enddate) -> DataFrame:
    # Get raw pricing data
    pricesource = fdm.Tushare().daily_price()
    df = pricesource.query(startdate=startdate, enddate=enddate, fields=[
        'ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'vol', 'amount'])
    if df.empty:
        return df
    # Data Preprocessing
    df.index = [df['ts_code'], df['trade_date']]
    del df['ts_code']
    del df['trade_date']
    df['vwap'] = df['amount']/df['vol']*10
    del df['amount']
    del df['vol']
    # Get adjust factor
    pricesource = fdm.Tushare().daily_adj()
    adf = pricesource.query(startdate=startdate, enddate=enddate)
    adf.index = [adf['ts_code'], adf['trade_date']]
    # Data Preprocessing
    del adf['ts_code']
    del adf['trade_date']

    df = df.join(adf)
    del adf

    for i in ['open', 'high', 'low', 'close', 'vwap']:
        df[i] = df[i]*df['adj_factor']
    df.reset_index(inplace=True)
    df.rename(columns={'ts_code': 'code',
                       'trade_date': 'date'}, inplace=True)
    return df
