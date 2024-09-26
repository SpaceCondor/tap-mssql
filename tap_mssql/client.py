"""SQL client handling.

This includes MSSQLStream and MSSQLConnector.
"""

from __future__ import annotations

import datetime
import typing as t
from functools import cached_property

import sqlalchemy as sa
from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._state import increment_state
from sqlalchemy import URL, text

if t.TYPE_CHECKING:
    from singer_sdk.helpers import types
    from singer_sdk.helpers.types import Context


class MSSQLConnector(SQLConnector):
    """Connects to the MSSQL SQL source."""

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Generates the SQLAlchemy URL.

        Returns:
            The generated SQLAlchemy URL.
        """
        if config.get("sqlalchemy_url_query"):
            return config.get("sqlalchemy_url_query")

        connection_url = URL.create(
            "mssql+pyodbc",
            username=config.get("username"),
            password=config.get("password"),
            host=config.get("host"),
            port=config.get("port"),
            database=config.get("database"),
            query={
                option["key"]: option["value"] for option in config.get("sqlalchemy_url_query_options", [])  # noqa: E501
            }
        )

        return connection_url.render_as_string(hide_password=False)

    @cached_property
    def database_change_tracking_enabled(self) -> bool:
        """Returns if the database is enabled for change tracking.

        Returns:
            True if the database is enabled for change tracking, False otherwise.
        """
        with self._connect() as conn:
            return conn.execute(
                text(
                    "SELECT 1 FROM sys.change_tracking_databases ctb "
                    "INNER JOIN sys.databases dbs ON ctb.database_id = dbs.database_id "
                    "WHERE dbs.name = :db"
                ),
                {
                    "db": self._engine.url.database
                }
            ).first() is not None

    @cached_property
    def change_tracking_tables(self) -> list[str]:
        """Returns if the database is enabled for change tracking.

        Returns:
            True if the database is enabled for change tracking, False otherwise.
        """
        with self._connect() as conn:
            return [
                r[0] for r in
                conn.execute(
                    text(
                        "SELECT OBJECT_NAME(object_id) AS table_name FROM sys.change_tracking_tables"  # noqa: E501
                    ))
            ]

    def get_minimum_valid_version(self, table_name: str) -> int:
        """Returns the minimum valid version of a table as reported by SQL Server.

        Returns:
            The minimum valid version of the table.
        """
        with self._connect() as conn:
            return conn.execute(
                text(
                    "SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(:table_name))"
                ),
                {
                    "table_name": table_name
                }
            ).first()[0]

    @property
    def change_tracking_current_version(self) -> int:
        """Returns the current change tracking version of the connected database.

        Returns:
            The current change tracking version.
        """
        with self._connect() as conn:
            return conn.execute(
                text(
                    "SELECT CHANGE_TRACKING_CURRENT_VERSION()"
                )
            ).first()[0]

class MSSQLStream(SQLStream):
    """Stream class for MSSQL streams."""

    connector_class = MSSQLConnector
    supports_nulls_first = False

    # Get records from stream
    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            raise NotImplementedError(msg)

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            order_by = (
                sa.nulls_first(replication_key_col.asc())
                if self.supports_nulls_first
                else replication_key_col.asc()
            )
            query = query.order_by(order_by)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        if self.ABORT_AT_RECORD_COUNT is not None:
            # Limit record count to one greater than the abort threshold. This ensures
            # `MaxRecordsLimitException` exception is properly raised by caller
            # `Stream._sync_records()` if more records are available than can be
            # processed.
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    continue
                yield transformed_record


class MSSQLChangeTrackingStream(SQLStream):
    """Stream class for MSSQL streams."""

    connector_class = MSSQLConnector
    supports_nulls_first = False

    replication_key = "_sdc_change_version"

    @cached_property
    def schema(self) -> dict:
        """Appends _sdc_deleted_at and _sdc_change_version to the schema.

        Returns:
            The dict.
        """
        schema_dict = t.cast(dict, self._singer_catalog_entry.schema.to_dict())

        schema_dict["properties"].update({
            "_sdc_deleted_at": {"type": ["string", "null"]}
        })

        schema_dict["properties"].update({
            "_sdc_change_version": {"type": ["integer", "null"]}
        })
        return schema_dict

    @cached_property
    def change_tracking_current_version(self) -> int | None:
        """Returns the current change tracking version of the connected database.

        Returns:
            The current change tracking version.
        """
        return self.connector.change_tracking_current_version

    @cached_property
    def minimum_valid_version(self) -> int | None:
        """Returns the minimum valid version of a table as reported by SQL Server.

        Returns:
            The minimum valid version of the table.
        """
        return self.connector.get_minimum_valid_version(str(self.fully_qualified_name))

    @cached_property
    def table_is_change_tracking_enabled(self) -> bool:
        """Checks if the current table is enabled for change tracking.

        Returns:
            True if the current table is enabled for change tracking, False otherwise.
        """
        mssql_connector = t.cast(MSSQLConnector, self.connector)
        table_name = mssql_connector.parse_full_table_name(self.fully_qualified_name)[2]
        return table_name in mssql_connector.change_tracking_tables

    @property
    def connector(self) -> MSSQLConnector:
        """Return the connector object.

        Returns:
            The connector object.
        """
        return t.cast(MSSQLConnector, self._connector)


    def get_records(self, context: Context | None) -> t.Iterable[dict[str, t.Any]]:  # noqa: C901, PLR0912
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        bookmark: int = self.get_starting_replication_key_value(context=context)
        minimum_valid_version = self.minimum_valid_version
        change_tracking_enabled = self.table_is_change_tracking_enabled

        using_change_tracking = True

        if not self.primary_keys:
            using_change_tracking = False
            self.logger.warning(
                "Table has no primary keys. Cannot use CHANGE_TRACKING. "
                "Executing a full table sync instead."
            )
        elif not change_tracking_enabled:
            using_change_tracking = False
            self.logger.warning(
                "Table is not enabled for CHANGE_TRACKING. "
                "Executing a full table sync instead."
            )
        elif not bookmark:
            using_change_tracking = False
            self.logger.warning(
                "There is no previous bookmark. Executing a full table sync."
            )
        elif bookmark < minimum_valid_version:
            using_change_tracking = False
            self.logger.warning(
                "CHANGE_TRACKING_MIN_VALID_VERSION has reported a value greater "
                "than current-log-version. Executing a full table sync."
            )

        selected_column_names = list(self.get_selected_schema()["properties"].keys())
        selected_column_names.remove("_sdc_deleted_at")
        selected_column_names.remove("_sdc_change_version")

        if not using_change_tracking:
            table = self.connector.get_table(
                full_table_name=self.fully_qualified_name,
                column_names=selected_column_names,
            )

            query = table.select()

            if self.replication_key:
                replication_key_col = table.columns[self.replication_key]
                order_by = (
                    sa.nulls_first(replication_key_col.asc())
                    if self.supports_nulls_first
                    else replication_key_col.asc()
                )
                query = query.order_by(order_by)

                if replication_key_col.type.python_type in (
                        datetime.datetime,
                        datetime.date
                ):
                    start_val = self.get_starting_timestamp(context)
                else:
                    start_val = self.get_starting_replication_key_value(context)

                if start_val:
                    query = query.where(replication_key_col >= start_val)

            if self.ABORT_AT_RECORD_COUNT is not None:
                # Limit record count to one greater than the abort threshold.
                # This ensures `MaxRecordsLimitException` exception is properly
                # raised by caller `Stream._sync_records()` if more records
                # are available than can be processed.
                query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)

        else:

            selected_columns = ", ".join(
                f"tb.{self.connector.quote(column)}" for column in selected_column_names
            )

            primary_key_conditions = " AND ".join(
                f"tb.{primary_key} = c.{primary_key}" for primary_key in self.primary_keys  # noqa: E501
            )


            query = text(
                f"""
                SELECT
                    c.SYS_CHANGE_VERSION AS _sdc_change_version,
                    c.SYS_CHANGE_OPERATION AS _sdc_deleted_at,
                    {selected_columns}
                FROM
                    CHANGETABLE (
                        CHANGES {self.connector.quote(str(self.fully_qualified_name))},
                        {self.change_tracking_current_version}
                    ) AS c
                LEFT JOIN
                    {self.connector.quote(str(self.fully_qualified_name))} AS tb
                ON
                    {primary_key_conditions}
                ORDER BY
                    c.SYS_CHANGE_VERSION ASC
                """ # noqa: S608, RUF100
            )

        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    continue
                yield transformed_record

    def post_process(
            self,
            row: types.Record,
            context: types.Context | None = None,  # noqa: ARG002
    ) -> dict | None:
        """Processes the record after extraction.

        Handles deleted records by updating _sdc_deleted_at.

        Returns:
            The modified record.
        """
        if "_sdc_deleted_at" not in row:
            row.update({"_sdc_deleted_at": None})

        if "_sdc_change_version" not in row:
            row.update({"_sdc_change_version": self.change_tracking_current_version})

        if row.get("_sdc_deleted_at", "") == "D":

            row.update(
                {
                    "_sdc_deleted_at": datetime.datetime.now(tz=datetime.timezone.utc)
                                                        .strftime(r"%Y-%m-%dT%H:%M:%SZ")
                }
            )

        else:
            row.update({"_sdc_deleted_at": None})

        return row

    def _increment_stream_state(
            self,
            latest_record: types.Record,
            *,
            context: types.Context | None = None,
    ) -> None:
        """Update state of stream or partition with data from the provided record.

        Raises `InvalidStreamSortException` is `self.is_sorted = True` and unsorted data
        is detected.

        Note: The default implementation does not advance any bookmarks unless
        `self.replication_method == 'INCREMENTAL'.

        Args:
            latest_record: TODO
            context: Stream partition or context dictionary.

        Raises:
            ValueError: TODO
        """
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if latest_record:  # This is the only line that has been overridden.
            if not self.replication_key:
                msg = (
                    f"Could not detect replication key for '{self.name}' "
                    f"stream(replication method={self.replication_method})"
                )
                raise ValueError(msg)
            treat_as_sorted = self.is_sorted
            if not treat_as_sorted and self.state_partitioning_keys is not None:
                # Streams with custom state partitioning are not resumable.
                treat_as_sorted = False
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record=latest_record,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )

    def _sync_records(
            self,
            context: types.Context | None = None,
            *,
            write_messages: bool = True,
    ) -> t.Generator[dict, t.Any, t.Any]:

        yield from super()._sync_records(context=context, write_messages=write_messages)

        if write_messages and self.selected:
            state_dict = self.get_context_state(context)
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record={
                    self.replication_key:
                        self.change_tracking_current_version
                },
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )
