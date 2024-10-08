"""Tests standard tap features using the built-in SDK tests library."""
from singer_sdk.testing import get_tap_test_class

from tap_mssql.tap import TapMSSQL
from tests.settings import SAMPLE_CONFIG_CORE

TestTapMSSQL = get_tap_test_class(
    tap_class=TapMSSQL,
    config=SAMPLE_CONFIG_CORE,
    catalog="tests/resources/persons_catalog.json",
)