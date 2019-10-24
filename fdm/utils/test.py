from datetime import datetime

import pandas as pd


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
