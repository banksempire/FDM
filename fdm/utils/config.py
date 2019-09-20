config: dict = {
    "mongodb": {
        "address": "192.168.56.1",
        "port": 27017
    },
    "Tushare": {
        "DBSetting": {
            "dbName": "tushareCache",
            "date_name": "trade_date",
            "code_name": "ts_code",
            "colSetting": {
                "DailyPrice": "dailyPricing",
                "DailyBasic": "dailyBasic",
                "DailyAdjFactor": "dailyAdjFactor"
            }
        }
    },
    "CleanData": {
        "DBSetting": {
            "dbName": "cleanData",
            "date_name": "date",
            "code_name": "code",
            "colSetting": {
                "Price": "price"
            }
        }
    },
    "Wind": {
        "DBSetting": {
            "dbName": "wind",
            "date_name": "date",
            "code_name": "code",
            "colSetting": {
                "EDB": "EDB"
            }
        }
    },
    "factorbase": {
        "DBSetting": {
            "dbName": "factorBase",
            "date_name": "date",
            "code_name": "code",
            "colSetting": {
                "size": "size"
            }
        }
    },
    "TempDB": {
        "DBSetting": {
            "dbName": "tempDB",
            "date_name": "date",
            "code_name": "code",
            "colSetting": {}
        }
    }
}
