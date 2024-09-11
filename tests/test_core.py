"""Tests standard tap features using the built-in SDK tests library."""
import sqlalchemy as sa
from faker import Faker
from singer_sdk.testing import get_tap_test_class
from testcontainers.mssql import SqlServerContainer

from tap_mssql.tap import TapMSSQL

sqlserver = SqlServerContainer(dialect="mssql+pyodbc")
sqlserver.start()

mssql_url_str = sqlserver.get_connection_url()
mssql_url = sa.make_url(mssql_url_str)
mssql_url = mssql_url.update_query_string(
    "driver=ODBC Driver 18 for SQL Server&TrustServerCertificate=Yes&autocommit=true&authentication=SqlPassword",
    True)

engine = sa.create_engine(mssql_url)
with engine.connect() as connection:
    connection.execute(sa.text("CREATE DATABASE singer"))
    connection.commit()

# Replace DB in the connection string
singer_url = mssql_url.render_as_string(hide_password=False).replace(mssql_url.database, "singer")

fake = Faker()
engine = sa.create_engine(singer_url)
with engine.connect() as connection:
    connection.execute(sa.text("""CREATE TABLE Persons (
                                    PersonID int IDENTITY(1,1) PRIMARY KEY,
                                    LastName varchar(255),
                                    FirstName varchar(255),
                                    Address varchar(255),
                                    City varchar(255)
                                );"""))
    connection.commit()

    for _ in range(10):
        connection.execute(sa.text("""INSERT INTO Persons (LastName, FirstName, Address, City)
                              VALUES (:lastname, :firstname, :address, :city)
                           """), {
            "lastname": fake.last_name(),
            "firstname": fake.first_name(),
            "address": fake.street_address(),
            "city": fake.city()
        })
        connection.commit()

SAMPLE_CONFIG = {
    "sqlalchemy_url_query": singer_url
}

TestTapMSSQL = get_tap_test_class(
    tap_class=TapMSSQL,
    config=SAMPLE_CONFIG
)


