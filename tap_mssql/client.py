"""SQL client handling.

This includes MSSQLStream and MSSQLConnector.
"""

from __future__ import annotations

import datetime
import typing as t
from functools import cached_property

import sqlalchemy
from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers import types
from singer_sdk.helpers._state import increment_state
from singer_sdk.helpers.types import Context
from sqlalchemy import URL, text


class MSSQLConnector(SQLConnector):
    """Connects to the MSSQL SQL source."""

    def get_sqlalchemy_url(self, config: dict) -> str:

        if config.get("sqlalchemy_url_query"):
            return config.get("sqlalchemy_url_query")

        connection_url = URL.create(
            "mssql+pyodbc",
            username=config.get("username"),
            password=config.get("password"),
            host=config.get("host"),
            port=config.get("port"),
            database=config.get("database"),
            query={option["key"]: option["value"] for option in config.get("sqlalchemy_url_query_options", [])}
        )

        return connection_url.render_as_string(hide_password=False)

    @cached_property
    def database_change_tracking_enabled(self) -> bool:
        """Returns if the database that the SQL Alchemy engine is connected to is enabled for change tracking.

        Returns:
            True if the database is enabled for change tracking, False otherwise.
        """
        with self._connect() as conn:
            return conn.execute(text('SELECT 1 FROM sys.change_tracking_databases ctdb ' +
                                     'INNER JOIN sys.databases dbs ON ctdb.database_id = dbs.database_id '
                                     'WHERE dbs.name = :db'),
                                {'db': self._engine.url.database}).first() is not None

    @cached_property
    def change_tracking_tables(self) -> t.List[str]:
        with self._connect() as conn:
            return [
                r[0] for r in
                conn.execute(text('SELECT OBJECT_NAME(object_id) AS table_name FROM sys.change_tracking_tables'))
            ]

    def get_minimum_valid_version(self, table_name: str) -> int:
        with self._connect() as conn:
            return conn.execute(text('SELECT CHANGE_TRACKING_MIN_VALID_VERSION(OBJECT_ID(:table_name))'),
                                {'table_name': table_name}
                                ).first()[0]

    @property
    def change_tracking_current_version(self) -> int:
        with self._connect() as conn:
            return conn.execute(text('SELECT CHANGE_TRACKING_CURRENT_VERSION()')
                                ).first()[0]

    @staticmethod
    def to_jsonschema_type(
            from_type: str
                       | sqlalchemy.types.TypeEngine
                       | type[sqlalchemy.types.TypeEngine],
    ) -> dict:

        """Returns a JSON Schema equivalent for the given SQL type.
        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """

        # This is taken from to_jsonschema_type() in typing.py
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
                from_type,
                sqlalchemy.types.TypeEngine,
        ):
            type_name = from_type.__name__
        else:  # pragma: no cover
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            # TODO: this should be a TypeError, but it's a breaking change.
            raise ValueError(msg)  # noqa: TRY004

        # Binary strings
        if type_name in ["BINARY", "IMAGE", "VARBINARY"]:
            return_type = {
                "type": ["string"],
            }

            return return_type

        if type_name in ["ROWVERSION", "TIMESTAMP"]:
            return {
                "type": ["string"],
            }

        # This is a MSSQL only DataType
        # SQLA does the conversion from 0,1
        # to Python True, False
        if type_name == "BIT":
            return {"type": ["boolean"]}

        # Strings
        if type_name in ["CHAR", "NCHAR", "VARCHAR", "NVARCHAR"]:
            return_type = {
                "type": ["string"],
            }

            # If there is a length field, set it
            if getattr(from_type, "length", False):
                return_type["maxLength"] = getattr(from_type, "length")

            return return_type

        # This is a MSSQL only DataType
        if type_name == "TINYINT":
            return {
                "type": ["integer"],
                "minimum": 0,
                "maximum": 255
            }

        if type_name == "SMALLINT":
            return {
                "type": ["integer"],
                "minimum": -32_768,
                "maximum": 32_767
            }

        if type_name == "INTEGER":
            return {
                "type": ["integer"],
                "minimum": -2_147_483_648,
                "maximum": 2_147_483_647
            }

        if type_name == "BIGINT":
            return {
                "type": ["integer"],
                "minimum": -9_223_372_036_854_775_808,
                "maximum": 9_223_372_036_854_775_807
            }

        if type_name in ("NUMERIC", "DECIMAL"):
            scale: int = getattr(from_type, "scale")

            if scale == 0:
                return {
                    "type": ["integer"]
                }

            return {
                "type": ["number"]
            }

        # This is a MSSQL only DataType
        if type_name == "SMALLMONEY":
            return {
                "type": ["number"],
                "minimum": -214748.3648,
                "maximum": 214748.3647
            }

        # This is a MSSQL only DataType
        # The min and max are getting truncated catalog
        if type_name == "MONEY":
            return {
                "type": ["number"],
                "minimum": -922_337_203_685_477.5808,
                "maximum": 922_337_203_685_477.5807
            }

        if type_name == "FLOAT":
            return {
                "type": ["number"],
                "minimum": -1.79e308,
                "maximum": 1.79e308
            }

        if type_name == "REAL":
            return {
                "type": ["number"],
                "minimum": -3.40e38,
                "maximum": 3.40e38
            }

        # Fall back to default for other types
        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            jsonschema_type: A dict

        Returns:
            SQLAlchemy type
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        return SQLConnector.to_sql_type(jsonschema_type)


class MSSQLStream(SQLStream):
    """Stream class for MSSQL streams."""
    connector_class = MSSQLConnector


class MSSQLChangeTrackingStream(SQLStream):
    """Stream class for MSSQL streams."""

    replication_key = "_sdc_change_version"
    connector_class = MSSQLConnector

    @cached_property
    def schema(self) -> dict:
        schema_dict = t.cast(dict, self._singer_catalog_entry.schema.to_dict())
        schema_dict["properties"].update({"_sdc_deleted_at": {"type": ["string", "null"]}})
        schema_dict["properties"].update({"_sdc_change_version": {"type": ["integer", "null"]}})
        return schema_dict

    @cached_property
    def change_tracking_current_version(self):
        return t.cast(MSSQLConnector, self.connector).change_tracking_current_version

    @cached_property
    def minimum_valid_version(self):
        return t.cast(MSSQLConnector, self.connector).get_minimum_valid_version(
            self.fully_qualified_name.table)

    @cached_property
    def table_is_change_tracking_enabled(self) -> bool:
        mssql_connector = t.cast(MSSQLConnector, self.connector)
        table_name = mssql_connector.parse_full_table_name(self.fully_qualified_name)[2]
        return table_name in mssql_connector.change_tracking_tables



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

        bookmark: int = self.get_starting_replication_key_value(context=context)
        minimum_valid_version = self.minimum_valid_version
        change_tracking_enabled = self.table_is_change_tracking_enabled

        using_change_tracking = True

        if not self.primary_keys:
            using_change_tracking = False
            self.logger.warning("Table has no primary keys. Cannot use CHANGE_TRACKING. " +
                                "Executing a full table sync instead.")
        elif not change_tracking_enabled:
            using_change_tracking = False
            self.logger.warning("Table is not enabled for CHANGE_TRACKING. Executing a full table sync instead.")
        elif not bookmark:
            using_change_tracking = False
            self.logger.warning(
                "There is no previous bookmark. Executing a full table sync."
            )
        elif bookmark < minimum_valid_version:
            using_change_tracking = False
            self.logger.warning(
                "CHANGE_TRACKING_MIN_VALID_VERSION has reported a value greater than current-log-version. Executing a "
                "full table sync."
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
        else:

            query = text(
                'SELECT c.SYS_CHANGE_VERSION as _sdc_change_version, c.SYS_CHANGE_OPERATION as _sdc_deleted_at, ' +
                ', '.join("tb." + self.connector.quote(selected_column) for selected_column in selected_column_names) +
                ' FROM CHANGETABLE (CHANGES ' + self.connector.quote(self.fully_qualified_name) + ', ' +
                str(self.change_tracking_current_version) + ') as c' +
                ' LEFT JOIN ' + self.connector.quote(self.fully_qualified_name) + ' AS tb ON ' +
                ' AND '.join('tb.' + primary_key + ' = ' + 'c.' + primary_key for primary_key in self.primary_keys) +
                ' ORDER BY c.SYS_CHANGE_VERSION ASC'
            )

        with self.connector._connect() as conn:  # noqa: SLF001
            for record in conn.execute(query).mappings():
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    continue
                yield transformed_record

    def post_process(  # noqa: PLR6301
            self,
            row: types.Record,
            context: types.Context | None = None,  # noqa: ARG002
    ) -> dict | None:

        if "_sdc_deleted_at" not in row:
            row.update({'_sdc_deleted_at': None})

        if "_sdc_change_version" not in row:
            row.update({'_sdc_change_version': self.change_tracking_current_version})

        if row.get("_sdc_deleted_at", "") == 'D':
            for key in row:
                if key not in self.primary_keys and key != '_sdc_change_version':
                    row.update({key: None})

            row.update(
                {
                    "_sdc_deleted_at": datetime.datetime.utcnow().strftime(
                        r"%Y-%m-%dT%H:%M:%SZ"
                    )
                }
            )

        else:
            row.update({'_sdc_deleted_at': None})

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

    def _sync_records(  # noqa: C901
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
                latest_record={self.replication_key: self.change_tracking_current_version},
                is_sorted=self.is_sorted,
                check_sorted=self.check_sorted,
            )
