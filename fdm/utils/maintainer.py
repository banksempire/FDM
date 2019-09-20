import fdm


def update_all():
    fdm.Wind.edb().update()
    fdm.Tushare.daily_adj().update()
    fdm.Tushare.daily_basic().update()
    fdm.Tushare.daily_price().update()
