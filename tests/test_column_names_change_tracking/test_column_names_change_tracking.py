"""Tests standard tap features using the built-in SDK tests library with a table containing special column names
."""
import sqlalchemy as sa
from singer_sdk.testing import TapTestRunner

from tap_mssql.tap import TapMSSQL
from tests.settings import SAMPLE_CONFIG_COLUMN_NAMES_CHANGE_TRACKING


def test_initial_sync(db_connection):
    """Check that the entire table is replication on a first sync"""

    person_ids = range(50)
    for person_id in person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO 
                        melty_column_names_ct.dbo.Persons 
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
        db_connection.commit()


    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_COLUMN_NAMES_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking_column_names.json",
    )
    test_runner.sync_all()
    assert len(test_runner.record_messages) == 50

    all_person_ids_in_records = [ person["record"]["PersonID"] for person in test_runner.record_messages ]
    assert set(all_person_ids_in_records).issuperset(person_ids)

def test_record_addition(db_connection):
    """Check that new records are properly emitted after an initial sync"""

    person_ids = range(50)
    for person_id in person_ids:
        db_connection.execute(
            sa.text(
                """
                    INSERT INTO 
                        melty_column_names_ct.dbo.Persons 
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
        db_connection.commit()


    test_runner = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_COLUMN_NAMES_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking_column_names.json",
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
                    INSERT INTO 
                        melty_column_names_ct.dbo.Persons 
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
        db_connection.commit()

    test_runner_after_insert = TapTestRunner(
        tap_class=TapMSSQL,
        config=SAMPLE_CONFIG_COLUMN_NAMES_CHANGE_TRACKING,
        catalog="tests/resources/persons_catalog_change_tracking_column_names.json",
        state=test_runner.state_messages[0]["value"]
    )

    test_runner_after_insert.sync_all()

    # Only 3 records should be emitted
    assert len(test_runner_after_insert.record_messages) == 3
    all_person_ids_in_new_records = [person["record"]["PersonID"] for person in test_runner_after_insert.record_messages]
    assert set(all_person_ids_in_new_records).issuperset(new_person_ids)
    assert set(all_person_ids_in_new_records).isdisjoint(all_person_ids_in_records)
