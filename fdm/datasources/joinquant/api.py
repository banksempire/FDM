from datetime import datetime, timedelta
import json
from functools import wraps
from io import StringIO

import requests as r
import pandas as pd
from pandas import DataFrame


class NonAuthorizedError(Exception):
    pass


class JQDataAPI:
    __URL__ = 'https://dataapi.joinquant.com/apis'
    __TOKEN__ = ''

    __C_TYPE = (
        'get_security_info',
        'get_industries',
    )

    __CD_TYPE = (
        'get_all_securities',
        'get_index_weights',
        'get_industry',
    )

    __CSE_TYPE = (
        'get_locked_shares',
        'get_mtss',
        'get_money_flow',
        'get_billboard_list',
    )

    __CDL_TYPE = (
        'get_index_stocks',
        'get_industry_stocks',
        'get_concept_stocks',
        'get_future_contracts',
        'get_dominant_future',
    )
    # ----------Decorators----------

    def _ensure_auth(func):
        @wraps(func)
        def inner(*args, **kwargs):
            if JQDataAPI.__TOKEN__ == '':
                raise NonAuthorizedError('Run JQDataAPI.auth first.')
            return func(*args, **kwargs)
        return inner
    # ----------Utils----------
    @classmethod
    def auth(cls, user, password):
        body = {
            "method": "get_token",
            "mob": user,
            "pwd": password,
        }
        response = r.post(cls.__URL__, data=json.dumps(body))
        cls.__TOKEN__ = response.text

    @classmethod
    def get_query_count(cls):
        body = {
            "method": "get_query_count",
            "token": cls.__TOKEN__
        }
        response = r.post(cls.__URL__, data=json.dumps(body))
        return response.text
    # ----------Generic Dataloaders----------
    @_ensure_auth
    def _c_generic(self, method):
        def func(code: str) -> DataFrame:
            body = {
                "method": method,
                "token": self.__TOKEN__,
                'code': code,
            }
            response = r.post(self.__URL__, data=json.dumps(body))
            df = pd.read_csv(StringIO(response.text))
            return df
        return func

    @_ensure_auth
    def _cd_generic(self, method):
        def func(code: str,
                 date: datetime) -> DataFrame:
            d = date.strftime('%Y-%m-%d') if not date is None else ''
            body = {
                "method": method,
                "token": self.__TOKEN__,
                'code': code,
                'date': d,
            }
            response = r.post(self.__URL__, data=json.dumps(body))
            df = pd.read_csv(StringIO(response.text))
            return df
        return func

    @_ensure_auth
    def _cdl_generic(self, method):
        def func(code: str,
                 date: datetime) -> DataFrame:
            d = date.strftime('%Y-%m-%d') if not date is None else ''
            body = {
                "method": method,
                "token": self.__TOKEN__,
                'code': code,
                'date': d,
            }
            response = r.post(self.__URL__, data=json.dumps(body))
            return response.text.split('\n')
        return func

    @_ensure_auth
    def _cse_generic(self, method):
        def func(code: str,
                 start: datetime,
                 end: datetime) -> DataFrame:
            s = start.strftime('%Y-%m-%d') if not start is None else ''
            e = end.strftime('%Y-%m-%d') if not end is None else ''
            body = {
                "method": method,
                "token": self.__TOKEN__,
                'code': code,
                'date': s,
                'enddate': e
            }
            response = r.post(self.__URL__, data=json.dumps(body))
            df = pd.read_csv(StringIO(response.text))
            return df
        return func

    def __getattr__(self, method):
        if method in self.__CSE_TYPE:
            return self._cse_generic(method)
        elif method in self.__CD_TYPE:
            return self._cd_generic(method)
        elif method in self.__CDL_TYPE:
            return self._cdl_generic(method)
        elif method in self.__C_TYPE:
            return self._c_generic(method)
        else:
            raise AttributeError('JQDataAPI has not method:', method)

    # ----------Dataloader----------
    @_ensure_auth
    def get_price_period(self, code: str,
                         start: datetime,
                         end: datetime) -> DataFrame:
        body = {
            "method": 'get_price_period',
            "token": self.__TOKEN__,
            "unit": "1d",
            'code': code,
            'date': start.strftime('%Y-%m-%d'),
            'end_date': end.strftime('%Y-%m-%d'),
            "fq_ref_date": ""
        }
        response = r.post(self.__URL__, data=json.dumps(body))
        df = pd.read_csv(StringIO(response.text))
        return df

    @_ensure_auth
    def get_financial_statement(self, code: str,
                                table: str,
                                start: datetime,
                                end: datetime) -> DataFrame:
        cond = "end_date#>=#{s}&end_date#<=#{e}&code#=#{c}&report_type#=#0".format(
            s=start.strftime('%Y-%m-%d'), e=end.strftime('%Y-%m-%d'), c=code)
        body = {
            "method": "run_query",
            "token": self.__TOKEN__,
            "table": 'finance.'+table,
            "columns": "",
            "conditions": cond,
            "count": 1000
        }
        response = r.post(self.__URL__, data=json.dumps(body))
        df = pd.read_csv(StringIO(response.text))
        return df
