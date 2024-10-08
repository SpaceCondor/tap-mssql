import sqlalchemy as sa
from faker import Faker
from singer_sdk.testing import TapTestRunner

from tap_mssql.tap import TapMSSQL
from tests.settings import SAMPLE_CONFIG_CHANGE_TRACKING


def test_initial_sync(db_connection):
    """Check that the entire table is replication on a first sync"""
    fake = Faker()

    person_ids = range(50)
    for person_id in person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO melty_ct.dbo.Persons (PersonID, FirstName)
                    VALUES (:personid, :firstname)
                """
            ), {
                'personid': person_id,
                "firstname": fake.first_name(),
            })
        db_connection.commit()


    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
    )
    test_runner.sync_all()
    assert len(test_runner.record_messages) == 50

    all_person_ids_in_records = [ person["record"]["PersonID"] for person in test_runner.record_messages ]
    assert set(all_person_ids_in_records).issuperset(person_ids)

def test_record_addition(db_connection):
    """Check that new records are properly emitted after an initial sync"""
    fake = Faker()

    person_ids = range(50)
    for person_id in person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO melty_ct.dbo.Persons (PersonID, FirstName)
                    VALUES (:personid, :firstname)
                """
            ), {
                'personid': person_id,
                "firstname": fake.first_name(),
            })
        db_connection.commit()


    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
    )
    test_runner.sync_all()
    assert len(test_runner.record_messages) == 50

    all_person_ids_in_records = [ person["record"]["PersonID"] for person in test_runner.record_messages ]
    assert set(all_person_ids_in_records).issuperset(person_ids)

    new_person_ids = range(50, 53)
    for person_id in new_person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO melty_ct.dbo.Persons (PersonID, FirstName)
                    VALUES (:personid, :firstname)
                """
            ), {
                'personid': person_id,
                "firstname": fake.first_name(),
            })
        db_connection.commit()

    test_runner_after_insert = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
        state=test_runner.state_messages[0]["value"]
    )

    test_runner_after_insert.sync_all()

    # Only 3 records should be emitted
    assert len(test_runner_after_insert.record_messages) == 3
    all_person_ids_in_new_records = [person["record"]["PersonID"] for person in test_runner_after_insert.record_messages]
    assert set(all_person_ids_in_new_records).issuperset(new_person_ids)
    assert set(all_person_ids_in_new_records).isdisjoint(all_person_ids_in_records)

def test_empty_db(db_connection):
    """Check that there are no issues if a db is empty"""
    fake = Faker()

    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
    )
    test_runner.sync_all()
    assert len(test_runner.record_messages) == 0
    new_person_ids = range(3)

    for person_id in new_person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO melty_ct.dbo.Persons (PersonID, FirstName)
                    VALUES (:personid, :firstname)
                """
            ), {
                'personid': person_id,
                "firstname": fake.first_name(),
            })
        db_connection.commit()

    test_runner_after_insert = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
        state=test_runner.state_messages[0]["value"]
    )

    test_runner_after_insert.sync_all()

    # Only 3 records should be emitted
    assert len(test_runner_after_insert.record_messages) == 3

def test_record_deletion(db_connection):
    """Check that record deletion performs as expected"""
    fake = Faker()

    person_ids = range(50)
    for person_id in person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO melty_ct.dbo.Persons (PersonID, FirstName)
                    VALUES (:personid, :firstname)
                """
            ), {
                'personid': person_id,
                "firstname": fake.first_name(),
            })
        db_connection.commit()


    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
    )
    test_runner.sync_all()
    assert len(test_runner.record_messages) == 50

    db_connection.execute(
        sa.text("DELETE FROM melty_ct.dbo.Persons WHERE PersonID = 2"))
    db_connection.commit()

    test_runner_after_delete = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking.json",
        state=test_runner.state_messages[0]["value"]
    )

    test_runner_after_delete.sync_all()

    # Test that the deleted record is correct
    assert len(test_runner_after_delete.record_messages) == 1
    assert test_runner_after_delete.record_messages[0]["record"]["PersonID"] == 2
    assert test_runner_after_delete.record_messages[0]["record"]["_sdc_deleted_at"] is not None
    assert test_runner_after_delete.record_messages[0]["record"]["FirstName"] is None
