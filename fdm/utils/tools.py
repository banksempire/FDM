from datetime import datetime

import pandas as pd


def prev_date(date: datetime, freq: str) -> datetime:
    return pd.date_range(end=date, periods=2, freq=freq)[0].to_pydatetime()


def next_date(date: datetime, freq: str) -> datetime:
    return pd.date_range(start=date, periods=2, freq=freq)[1].to_pydatetime()


def test_feeder_func(code, field, start, end):
    def gen():
        for i in pd.date_range(start, end):
            value = code + field + i.strftime('%Y%m%d')
            doc = {
                'date': i,
                'code': code,
                field: value,
            }
            yield pd.Series(doc)
    return pd.DataFrame(gen())
