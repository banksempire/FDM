from datetime import datetime

import pandas as pd


def test_feeder_func(cls, code, field, start, end):

    return ord_test_feeder_func(code, field, start, end)


def ord_test_feeder_func(code, field, start, end):
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


def test_feeder_func_Q(cls, code, field, start, end):
    def gen():
        for i in pd.date_range(start, end, freq='Q'):
            value = code + field + i.strftime('%Y%m%d')
            doc = {
                'date': i,
                'code': code,
                field: value,
            }
            yield pd.Series(doc)
    return pd.DataFrame(gen())


test_config = {
    "Test": {
        "DBSetting": {
            "dbName": "Test",
            "date_name": "date",
            "code_name": "code",
            "colSetting": {
                "Price": "price"
            }
        }
    },
}
