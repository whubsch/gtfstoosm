"""
GTFS data handling module.

This module provides functionality for parsing and processing GTFS data.
It handles the reading and validation of GTFS feeds.
"""

import logging
import zipfile
from io import BytesIO

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class GTFSFeed(BaseModel):
    """Class for storing and querying a GTFS feed."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Allow Polars DataFrames
        validate_assignment=True,
    )

    feed_dir: str
    tables: dict[str, pl.DataFrame] = Field(default_factory=dict)
    name: str | None = None
    required_files: list[str] = Field(
        default_factory=lambda: [
            "agency.txt",
            "stops.txt",
            "routes.txt",
            "trips.txt",
            "stop_times.txt",
            "shapes.txt",
        ]
    )

    def load(self) -> None:
        """
        Load a GTFS feed.
        """

        # Read all files in the feed directory
        with zipfile.ZipFile(self.feed_dir, "r") as zip_ref:
            for file in zip_ref.namelist():
                if file not in self.required_files:
                    logger.debug(f"Skipping optional file {file}")
                    continue
                table_name = file[:-4]  # Remove the .txt extension
                try:
                    logger.debug(f"Loading {file}")
                    with zip_ref.open(file) as file_obj:
                        # Read the CSV data into a polars DataFrame
                        df = pl.read_csv(
                            BytesIO(file_obj.read()), infer_schema_length=None
                        )

                        logger.info(f"Loaded {df.height:,} records from {file}")
                        self.tables[table_name] = df

                except zipfile.BadZipFile as err:
                    raise ValueError(
                        f"The file at {self.feed_dir} is not a valid zip file"
                    ) from err
