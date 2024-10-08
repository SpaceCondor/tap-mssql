import pytest
import sqlalchemy as sa
from faker import Faker

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
    connection.execute(sa.text("CREATE DATABASE melty"))
    connection.commit()

    connection.execute(sa.text("""CREATE TABLE melty.dbo.Persons (
                                        PersonID int PRIMARY KEY,
                                        FirstName varchar(255),
                                    );"""))
    connection.commit()


def drop_db(connection):
    connection.execute(sa.text(
        "ALTER DATABASE melty SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE melty"))
    connection.commit()

def seed_db(connection):
    fake = Faker()
    person_ids = range(50)
    for person_id in person_ids:
        connection.execute(
            sa.text(
                """
                    INSERT INTO melty.dbo.Persons (PersonID, FirstName)
                    VALUES (:personid, :firstname)
                """
            ), {
                'personid': person_id,
                "firstname": fake.first_name(),
            })
        connection.commit()