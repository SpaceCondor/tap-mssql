import pytest
import sqlalchemy as sa

from tests.settings import DB_SQLALCHEMY_URL


@pytest.fixture(scope="function")
def db_connection():
    engine = sa.create_engine(DB_SQLALCHEMY_URL)
    """Fixture to connect with DB."""
    connection = engine.connect()

    create_db(connection)

    yield connection

    drop_db(connection)
    connection.close()


def create_db(connection):
    connection.execute(sa.text("CREATE DATABASE melty_ct"))
    connection.commit()

    connection.execute(sa.text(
        "ALTER DATABASE melty_ct SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"))
    connection.commit()

    connection.execute(sa.text("""CREATE TABLE melty_ct.dbo.Persons (
                                        PersonID int PRIMARY KEY,
                                        FirstName varchar(255),
                                    );"""))
    connection.commit()

    connection.execute(sa.text("""ALTER TABLE melty_ct.dbo.Persons  
                                  ENABLE CHANGE_TRACKING  
                                  WITH (TRACK_COLUMNS_UPDATED = ON) 
                                  """))
    connection.commit()


def drop_db(connection):
    connection.execute(sa.text(
        "ALTER DATABASE melty_ct SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE melty_ct"))
    connection.commit()
