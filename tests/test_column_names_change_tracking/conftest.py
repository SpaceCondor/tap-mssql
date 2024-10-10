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
    connection.execute(sa.text("CREATE DATABASE melty_column_names_ct"))
    connection.commit()

    connection.execute(sa.text(
        "ALTER DATABASE melty_column_names_ct SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 2 DAYS, AUTO_CLEANUP = ON)"))
    connection.commit()



    connection.execute(sa.text("""CREATE TABLE melty_column_names_ct.dbo.Persons (
                                        PersonID int PRIMARY KEY,
                                        [External] varchar(1),
                                        ä varchar(1),
                                        _data varchar(1),
                                        [#oid] varchar(1),
                                        [S p a c e] varchar(1),
                                        "[brackets]" varchar(1),
                                        ["QUOTE"] varchar(1),
                                        [TABLE] varchar(1)
                                    );"""))
    connection.commit()

    connection.execute(sa.text("""ALTER TABLE melty_column_names_ct.dbo.Persons  
                                      ENABLE CHANGE_TRACKING  
                                      WITH (TRACK_COLUMNS_UPDATED = ON) 
                                      """))
    connection.commit()


def drop_db(connection):
    connection.execute(sa.text(
        "ALTER DATABASE melty_column_names_ct SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE melty_column_names_ct"))
    connection.commit()
