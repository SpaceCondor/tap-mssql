"""MSSQL tap class."""

from __future__ import annotations

import copy
from typing import Sequence

from singer_sdk import SQLStream, SQLTap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk._singerlib import Catalog, Metadata, Schema

from tap_mssql.client import MSSQLChangeTrackingStream, MSSQLStream


class TapMSSQL(SQLTap):
    """MSSQL tap class."""

    name = "tap-mssql"
    default_stream_class = MSSQLStream

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description="Host for SQL Server Instance."
        ),
        th.Property(
            "database",
            th.StringType,
            description="Database to connect to."
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=1433,
            description="The port of the SQL Server Instance."
        ),
        th.Property(
            "username",
            th.StringType,
            description="Username used to authenticate."
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description="Password used to authenticate."
        ),
        th.Property(
            "sqlalchemy_url_query_options",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "key",
                        th.StringType,
                        description=(
                            "Key of the sqlalchemy URL query option. "
                            "Example: driver"
                        )
                    ),
                    th.Property(
                        "value",
                        th.StringType,
                        description=(
                            "Value of the sqlalchemy URL query option. "
                            "Example: ODBC Driver 18 for SQL Server"
                        )
                    )
                )
            ),
            description=("List of SQLAlchemy URL Query options to provide. Example: "
                         "driver, TrustServerCertificate, etc.")
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.StringType,
            secret=True,
            description=(
                "SQLAlchemy URL. Setting this will take "
                "precedence over other connection settings."
            )
        ),
        th.Property(
            "default_replication_method",
            th.StringType,
            default="FULL_TABLE",
            allowed_values=["FULL_TABLE", "INCREMENTAL", "LOG_BASED"],
            description=(
                "Replication method to use if there is not a catalog entry to override "
                "this choice. One of `FULL_TABLE`, `INCREMENTAL`, or `LOG_BASED`."
            ),
        ),
    ).to_dict()

    @property
    def catalog(self) -> Catalog:  # noqa: C901
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        new_catalog: Catalog = Catalog()
        modified_streams: list = []
        for stream in super().catalog.streams:
            stream_modified = False
            new_stream = copy.deepcopy(stream)
            if (
                    new_stream.replication_method == "LOG_BASED"
                    and new_stream.schema.properties
            ):
                for schema_property in new_stream.schema.properties.values():
                    if "null" not in schema_property.type:
                        if isinstance(schema_property.type, list):
                            schema_property.type.append("null")
                        else:
                            schema_property.type = [schema_property.type, "null"]
                if new_stream.schema.required:
                    stream_modified = True
                    new_stream.schema.required = None
                if "_sdc_deleted_at" not in new_stream.schema.properties:
                    stream_modified = True

                    new_stream.schema.properties.update(
                        {"_sdc_deleted_at": Schema(type=["string", "null"])}
                    )

                    new_stream.metadata.update(
                        {
                            ("properties", "_sdc_deleted_at"): Metadata(
                                inclusion=Metadata.InclusionType.AVAILABLE,
                                selected=True
                            )
                        }
                    )
                if "_sdc_change_version" not in new_stream.schema.properties:
                    stream_modified = True

                    new_stream.schema.properties.update(
                        {"_sdc_change_version": Schema(type=["integer", "null"])}
                    )

                    new_stream.metadata.update(
                        {
                            ("properties", "_sdc_change_version"): Metadata(
                                inclusion=Metadata.InclusionType.AVAILABLE,
                                selected=True,
                                selected_by_default=None
                            )
                        }
                    )
            if stream_modified:
                modified_streams.append(new_stream.tap_stream_id)
            new_catalog.add_stream(new_stream)
        if modified_streams:
            self.logger.info(
                "One or more LOG_BASED catalog entries were modified "
                "(%s) to allow nullability and include _sdc columns. "
                "See README for further information.",
                modified_streams
            )
        return new_catalog

    def discover_streams(self) -> Sequence[Stream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        streams: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            if catalog_entry["replication_method"] == "LOG_BASED":
                streams.append(
                    MSSQLChangeTrackingStream(
                        self, catalog_entry, connector=self.tap_connector
                    )
                )
            else:
                streams.append(
                    MSSQLStream(self, catalog_entry, connector=self.tap_connector)
                )
        return streams


if __name__ == "__main__":
    TapMSSQL.cli()
