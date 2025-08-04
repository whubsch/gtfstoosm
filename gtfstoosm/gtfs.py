"""
GTFS data handling module.

This module provides functionality for parsing and processing GTFS data.
It handles the reading and validation of GTFS feeds.
"""

import polars as pl
import logging
import re
from io import BytesIO
import zipfile
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class GTFSFeed(BaseModel):
    """Class for storing and querying a GTFS feed."""

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
        ]
    )
    optional_files: list[str] = Field(
        default_factory=lambda: [
            "calendar.txt",
            "calendar_dates.txt",
            "fare_attributes.txt",
            "fare_rules.txt",
            "shapes.txt",
            "frequencies.txt",
            "transfers.txt",
            "pathways.txt",
            "levels.txt",
            "feed_info.txt",
            "translations.txt",
            "attributions.txt",
        ]
    )

    class Config:
        arbitrary_types_allowed = True  # Allow Polars DataFrames
        validate_assignment = True

    def load(self) -> None:
        """
        Load a GTFS feed.
        """

        # Read all files in the feed directory
        with zipfile.ZipFile(self.feed_dir, "r") as zip_ref:
            for file in zip_ref.namelist():
                table_name = file[:-4]  # Remove the .txt extension
                try:
                    with zip_ref.open(file) as file_obj:
                        # Read the CSV data into a polars DataFrame
                        df = pl.read_csv(
                            BytesIO(file_obj.read()), infer_schema_length=None
                        )

                        logger.info(f"Loaded {df.height:,} records from {file}")
                        self.tables[table_name] = df

                except zipfile.BadZipFile:
                    raise ValueError(
                        f"The file at {self.feed_dir} is not a valid zip file"
                    )
                except Exception as e:
                    raise ValueError(f"Error loading {file}: {str(e)}")

    def _read_csv_file(self, file_path: str) -> pl.DataFrame:
        """
        Read a CSV file using Polars.

        Args:
            file_path: Path to the CSV file.

        Returns:
            pl.DataFrame: Polars DataFrame containing the data from the CSV file.
        """
        try:
            # Read the CSV file using Polars
            df = pl.read_csv(
                file_path, infer_schema_length=None, null_values=[""], encoding="utf8"
            )

            # Clean values in the DataFrame
            for col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .map_elements(self._clean_value, return_dtype=pl.Utf8)
                    .alias(col)
                )

            return df

        except Exception as e:
            print(f"Error reading {file_path}: {e}")
            return pl.DataFrame()

    def _clean_value(self, value: str) -> str:
        """
        Clean a value from a GTFS feed.

        Args:
            value: The value to clean.

        Returns:
            The cleaned value.
        """
        if value is None:
            return ""

        # Convert to string
        if not isinstance(value, str):
            value = str(value)

        # Replace line breaks with spaces
        value = value.replace("\n", " ").replace("\r", " ")

        # Replace multiple spaces with a single space
        value = re.sub(r"\s+", " ", value)

        # Strip leading and trailing whitespace
        value = value.strip()

        return value

    def get_table(self, table_name) -> pl.DataFrame:
        """
        Get a table from the GTFS feed.

        Args:
            table_name: The name of the table to get.

        Returns:
            pl.DataFrame: Polars DataFrame containing the data from the table.
        """
        if table_name in self.tables:
            return self.tables[table_name]
        else:
            return pl.DataFrame()


class GTFSToOSMMapper:
    """Class for mapping GTFS data to OSM data."""

    @staticmethod
    def map_route_type_to_osm(route_type):
        """
        Map a GTFS route type to OSM tags.

        Args:
            route_type: The GTFS route type.

        Returns:
            dict: Dictionary containing OSM tags.
        """
        # Convert route_type to integer if it's a string
        if isinstance(route_type, str):
            try:
                route_type = int(route_type)
            except ValueError:
                return {"route": "unknown"}

        # Map GTFS route types to OSM route types
        # https://developers.google.com/transit/gtfs/reference#routestxt
        gtfs_to_osm = {
            0: {"route": "tram"},
            1: {"route": "subway"},
            2: {"route": "train"},
            3: {"route": "bus"},
            4: {"route": "ferry"},
            5: {"route": "tram", "tram": "cable_car"},
            6: {"route": "aerialway"},
            7: {"route": "funicular"},
            11: {"route": "trolleybus"},
            12: {"route": "monorail"},
        }

        # Return OSM tags for the route type
        return gtfs_to_osm.get(route_type, {"route": "unknown"})

    @staticmethod
    def map_stop_to_osm(stop, route_type=None):
        """
        Map a GTFS stop to OSM tags.

        Args:
            stop: The GTFS stop as a dictionary or DataFrame row.
            route_type: The GTFS route type.

        Returns:
            dict: Dictionary containing OSM tags.
        """
        # Convert Polars row to dictionary if necessary
        if hasattr(stop, "to_dict"):
            stop = {col: stop[col] for col in stop.keys()}

        # Start with basic tags
        tags = {
            "name": stop.get("stop_name", ""),
            "ref": stop.get("stop_id", ""),
            "public_transport": "stop_position",
        }

        # Add location type specific tags
        location_type = stop.get("location_type", "0")
        if location_type == "1":  # Station
            tags["public_transport"] = "station"
        elif location_type == "2":  # Entrance/Exit
            tags["public_transport"] = "entrance"
        elif location_type == "3":  # Generic Node
            tags["public_transport"] = "node"
        elif location_type == "4":  # Boarding Area
            tags["public_transport"] = "platform"

        # Add wheelchair accessibility
        wheelchair_boarding = stop.get("wheelchair_boarding", "")
        if wheelchair_boarding == "1":
            tags["wheelchair"] = "yes"
        elif wheelchair_boarding == "2":
            tags["wheelchair"] = "no"

        # Add route type specific tags
        if route_type is not None:
            route_tags = GTFSToOSMMapper.map_route_type_to_osm(route_type)
            if route_tags.get("route") == "bus":
                tags["highway"] = "bus_stop"
            elif route_tags.get("route") == "tram":
                tags["railway"] = "tram_stop"
            elif (
                route_tags.get("route") == "subway"
                or route_tags.get("route") == "train"
            ):
                tags["railway"] = "station"
            elif route_tags.get("route") == "ferry":
                tags["amenity"] = "ferry_terminal"

        return tags

    @staticmethod
    def map_route_to_osm(route, agency_name=None):
        """
        Map a GTFS route to OSM tags.

        Args:
            route: The GTFS route as a dictionary or DataFrame row.
            agency_name: The name of the agency.

        Returns:
            dict: Dictionary containing OSM tags.
        """
        # Convert Polars row to dictionary if necessary
        if hasattr(route, "to_dict"):
            route = {col: route[col] for col in route.keys()}

        # Start with basic tags
        tags = {
            "type": "route",
            "ref": route.get("route_short_name", ""),
            "name": route.get("route_long_name", ""),
        }

        # Add agency information
        if agency_name:
            tags["operator"] = agency_name

        # Add route type specific tags
        route_type = route.get("route_type", "")
        route_tags = GTFSToOSMMapper.map_route_type_to_osm(route_type)
        tags.update(route_tags)

        # Add color information
        if "route_color" in route and route["route_color"]:
            tags["colour"] = "#" + route["route_color"]

        # Add route URL if available
        if "route_url" in route and route["route_url"]:
            tags["website"] = route["route_url"]

        # Clean up empty tags
        tags = {k: v for k, v in tags.items() if v}

        return tags
