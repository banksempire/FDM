from datetime import datetime

import pandas as pd
from pandas import DataFrame


def prev_date(date: datetime, freq: str) -> datetime:
    return pd.date_range(end=date, periods=2, freq=freq)[0].to_pydatetime()


def next_date(date: datetime, freq: str) -> datetime:
    return pd.date_range(start=date, periods=2, freq=freq)[1].to_pydatetime()


def del_id(df) -> DataFrame:
    '''Delete column['_id'] from result'''
    if not df.empty:
        del df['_id']
        df.index = range(df.shape[0])
    return df


def mongodb_name_compliance(codes):
    if isinstance(codes, str):
        return codes.upper().replace('.', '~')
    else:
        return [v.upper().replace('.', '~') for v in codes]
