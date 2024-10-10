"""Tests standard tap features using the built-in SDK tests library with a table containing special column names
."""
from singer_sdk.testing import get_tap_test_class

from tap_mssql.tap import TapMSSQL
from tests.settings import SAMPLE_CONFIG_COLUMN_NAMES

TestTapMSSQLColumnNames = get_tap_test_class(
    tap_class=TapMSSQL,
    config=SAMPLE_CONFIG_COLUMN_NAMES,
    catalog="tests/resources/persons_catalog_column_names.json",
)