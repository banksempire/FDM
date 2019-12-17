from datetime import datetime, timedelta

import fdm
from fdm.utils.tools import normalize_dt


def update_all():
    fdm.Wind().edb().update()
    fdm.Tushare().daily_adj().update()
    fdm.Tushare().daily_basic().update()
    fdm.Tushare().daily_price().update()


def jqdata_update(user, password):
    from fdm.datasources.joinquant.api import JQDataAPI
    JQDataAPI.auth(user, password)

    data = JQDataAPI().get_all_securities('stock', None)
    codes = list(data['code'])
    end = normalize_dt(datetime.now() - timedelta(1))
    start = datetime(2004, 12, 20)

    methods = ['daily_price', 'income', 'cashflow',
               'balance', 'fin_income', 'fin_cashflow', 'fin_balance']
    jq = fdm.JQData()
    for method in methods:
        print('Running update method:', method)
        getattr(jq, method)().update(codes,
                                     start,
                                     end)
