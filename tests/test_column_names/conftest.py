import pytest
import sqlalchemy as sa

from tests.settings import DB_SQLALCHEMY_URL


@pytest.fixture(scope="module", autouse=True)
def db_connection():
    engine = sa.create_engine(DB_SQLALCHEMY_URL)
    """Fixture to connect with DB."""
    connection = engine.connect()

    create_db(connection)
    seed_db(connection)

    yield connection

    drop_db(connection)
    connection.close()


def create_db(connection):
    connection.execute(sa.text("CREATE DATABASE melty_column_names"))
    connection.commit()

    connection.execute(sa.text("""CREATE TABLE melty_column_names.dbo.Persons (
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


def drop_db(connection):
    connection.execute(sa.text(
        "ALTER DATABASE melty_column_names SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE melty_column_names"))
    connection.commit()


def seed_db(connection):
    person_ids = range(50)
    for person_id in person_ids:
        connection.execute(
            sa.text(
                """
                    INSERT INTO 
                        melty_column_names.dbo.Persons 
                        (PersonID, [External], ä, _data, [#oid], [S p a c e], "[brackets]", ["QUOTE"], [TABLE])
                    VALUES (:personid, :external, :a, :data, :oid, :space, :brackets, :quote, :table)
                """
            ), {
                "personid": person_id,
                "external": "a",
                "a": "b",
                "data": "c",
                "oid": "d",
                "space": "e",
                "brackets": "f",
                "quote": "g",
                "table": "h"
            })
        connection.commit()
