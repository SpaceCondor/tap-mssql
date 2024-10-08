import datetime

DB_SQLALCHEMY_URL = ("mssql+pyodbc://sa:!Melty8Melty!@localhost:1433/master?driver=ODBC+Driver+18+for+SQL+Server"
                     "&TrustServerCertificate=Yes&autocommit=true")

SAMPLE_CONFIG_CORE = {
    "host": "localhost",
    "port": 1433,
    "username": "sa",
    "password": "!Melty8Melty!",
    "database": "melty",
    "sqlalchemy_url_query_options": [
        {
            "key": "driver",
            "value": "ODBC Driver 18 for SQL Server"
        },
        {
            "key": "TrustServerCertificate",
            "value": "Yes"
        },
        {
            "key": "authentication",
            "value": "SqlPassword"
        },
        {
            "key": "autocommit",
            "value": "true"
        }
    ]
}

SAMPLE_CONFIG_CHANGE_TRACKING = {
    "host": "localhost",
    "port": 1433,
    "username": "sa",
    "password": "!Melty8Melty!",
    "database": "melty_ct",
    "sqlalchemy_url_query_options": [
        {
            "key": "driver",
            "value": "ODBC Driver 18 for SQL Server"
        },
        {
            "key": "TrustServerCertificate",
            "value": "Yes"
        },
        {
            "key": "authentication",
            "value": "SqlPassword"
        },
        {
            "key": "autocommit",
            "value": "true"
        }
    ]
}

SAMPLE_CONFIG_INCREMENTAL = {
    "host": "localhost",
    "port": 1433,
    "username": "sa",
    "password": "!Melty8Melty!",
    "database": "melty_inc",
    "sqlalchemy_url_query_options": [
        {
            "key": "driver",
            "value": "ODBC Driver 18 for SQL Server"
        },
        {
            "key": "TrustServerCertificate",
            "value": "Yes"
        },
        {
            "key": "authentication",
            "value": "SqlPassword"
        },
        {
            "key": "autocommit",
            "value": "true"
        }
    ],
    "start_date": datetime.datetime(2022, 11, 1).isoformat()
}