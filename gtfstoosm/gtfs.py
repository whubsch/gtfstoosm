"""
GTFS data handling module.

This module provides functionality for parsing and processing GTFS data.
It handles the reading and validation of GTFS feeds.
"""

import os
import polars as pl
import logging
import re
import tempfile
import zipfile

logger = logging.getLogger(__name__)


class GTFSFeed:
    """Class for storing and querying a GTFS feed."""

    def __init__(self, feed_dir=None):
        """
        Initialize a GTFS feed.

        Args:
            feed_dir: Path to the directory containing the GTFS feed files.
        """
        # Required GTFS files
        self.required_files = [
            "agency.txt",
            "stops.txt",
            "routes.txt",
            "trips.txt",
            "stop_times.txt",
        ]

        # Optional GTFS files
        self.optional_files = [
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

        # Dictionary to store the GTFS tables as Polars DataFrames
        self.tables = {}

        # Set the feed directory
        self.feed_dir = feed_dir

        # Load the feed if a feed directory is provided
        if feed_dir:
            self.load(feed_dir)

        # Set the name of the feed, use the agency name if available
        self.name = None
        if "agency" in self.tables:
            self.name = (
                self.tables["agency"]
                .filter(pl.col("agency_id") == "0")
                .select("agency_name")
                .item()
            )

    def validate(self):
        """
        Validate the GTFS feed.

        Checks that all required files are present and that they contain the required fields.

        Returns:
            list: A list of validation errors.
        """
        errors = []

        # Check that the feed directory exists
        if not self.feed_dir:
            errors.append("Feed directory not specified.")
            return errors

        if not os.path.exists(self.feed_dir):
            errors.append(f"Feed directory {self.feed_dir} does not exist.")
            return errors

        # Check that all required files are present
        for file in self.required_files:
            file_path = os.path.join(self.feed_dir, file)
            if not os.path.exists(file_path):
                errors.append(f"Required file {file} not found.")

        # Check that required fields are present in required files
        required_fields = {
            "agency.txt": ["agency_id", "agency_name", "agency_url", "agency_timezone"],
            "stops.txt": ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            "routes.txt": [
                "route_id",
                "route_short_name",
                "route_long_name",
                "route_type",
            ],
            "trips.txt": ["route_id", "service_id", "trip_id"],
            "stop_times.txt": [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
            ],
        }

        for file, fields in required_fields.items():
            file_path = os.path.join(self.feed_dir, file)
            if os.path.exists(file_path):
                try:
                    # Read the first row of the file using Polars to get the header
                    df = pl.read_csv(file_path, n_rows=1)
                    header = df.columns

                    # Check that all required fields are present
                    for field in fields:
                        if field not in header:
                            errors.append(
                                f"Required field {field} not found in {file}."
                            )
                except Exception as e:
                    errors.append(f"Error reading {file}: {e}")

        return errors

    def load(self, feed_dir=None):
        """
        Load a GTFS feed.

        Args:
            feed_dir: Path to the directory containing the GTFS feed files.
        """
        if feed_dir:
            self.feed_dir = feed_dir

        # Read all files in the feed directory
        files = os.listdir(self.feed_dir)
        for file in files:
            if self.feed_dir and file.endswith(".txt"):
                file_path = os.path.join(self.feed_dir, file)
                table_name = file[:-4]  # Remove the .txt extension
                try:
                    self.tables[table_name] = self._read_csv_file(file_path)
                except Exception as e:
                    print(f"Error reading {file}: {e}")

    def _read_csv_file(self, file_path):
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

    def _clean_value(self, value):
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

    def get_table(self, table_name):
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


class GTFSReader:
    """Class for reading GTFS feeds."""

    @staticmethod
    def read_feed(feed_path):
        """
        Read a GTFS feed.

        Args:
            feed_path: Path to the GTFS feed file or directory.

        Returns:
            GTFSFeed: A GTFSFeed object.
        """
        # If the feed path is a zip file, extract it to a temporary directory
        if feed_path.endswith(".zip"):
            temp_dir = tempfile.mkdtemp()
            with zipfile.ZipFile(feed_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            feed_dir = temp_dir
        else:
            feed_dir = feed_path

        # Create a GTFSFeed object
        feed = GTFSFeed(feed_dir)

        return feed

    @staticmethod
    def get_agency_name(feed):
        """
        Get the name of the agency in a GTFS feed.

        Args:
            feed: The GTFSFeed object.

        Returns:
            str: The name of the agency.
        """
        agency = feed.get_table("agency")
        if agency.is_empty():
            return None

        # Try to get the name of the agency with ID 0
        if "agency_id" in agency.columns:
            agency_0 = agency.filter(pl.col("agency_id") == "0")
            if not agency_0.is_empty() and "agency_name" in agency_0.columns:
                return agency_0.select("agency_name").item()

        # If there is no agency with ID 0, get the name of the first agency
        if "agency_name" in agency.columns:
            return agency.select("agency_name").item(0)

        return None
