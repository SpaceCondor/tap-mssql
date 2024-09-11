"""MSSQL entry point."""

from __future__ import annotations

from tap_mssql.tap import TapMSSQL

TapMSSQL.cli()
