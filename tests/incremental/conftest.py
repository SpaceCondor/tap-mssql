import datetime

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

    yield connection

    drop_db(connection)
    connection.close()


def create_db(connection):
    connection.execute(sa.text("CREATE DATABASE melty_inc"))
    connection.commit()

    connection.execute(sa.text("""CREATE TABLE melty_inc.dbo.Persons (
                                        PersonID int PRIMARY KEY,
                                        FirstName varchar(255),
                                        UpdatedAt datetimeoffset,
                                    );"""))
    connection.commit()


def drop_db(connection):
    connection.execute(sa.text(
        "ALTER DATABASE melty_inc SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE melty_inc"))
    connection.commit()