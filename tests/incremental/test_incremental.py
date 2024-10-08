import datetime

import sqlalchemy as sa
from faker import Faker
from singer_sdk.testing import TapTestRunner

from tap_mssql.tap import TapMSSQL
from tests.settings import SAMPLE_CONFIG_INCREMENTAL


def test_incremental(db_connection):
    """Check that incremental replication works given a start_date"""
    fake = Faker()
    db_connection.execute(
        sa.text(
            """
                INSERT INTO melty_inc.dbo.Persons (PersonID, FirstName, UpdatedAt)
                VALUES (:personid, :firstname, :updatedat)
            """
        ), {
            "personid": 1,
            "firstname": fake.first_name(),
            "updatedat": datetime.datetime(2022, 10, 20).isoformat()
        })
    db_connection.commit()
    db_connection.execute(
        sa.text(
            """
                INSERT INTO melty_inc.dbo.Persons (PersonID, FirstName, UpdatedAt)
                VALUES (:personid, :firstname, :updatedat)
            """
        ), {
            "personid": 2,
            "firstname": fake.first_name(),
            "updatedat": datetime.datetime(2022, 11, 20).isoformat()
        })

    db_connection.commit()

    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_INCREMENTAL,
        catalog="tests/resources/persons_catalog_incremental.json",
    )
    test_runner.sync_all()
    assert len(test_runner.record_messages) == 1