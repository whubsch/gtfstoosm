"""
GTFS data handling module.

This module provides functionality for parsing and processing GTFS data.
It handles the reading and validation of GTFS feeds.
"""

import os
import zipfile
import csv
import logging
from typing import Any
from datetime import datetime
import io

logger = logging.getLogger(__name__)


class GTFSFeed:
    """Class representing a GTFS feed with its data tables."""

    def __init__(self, feed_path: str):
        """
        Initialize a GTFS feed from a zip file.

        Args:
            feed_path: Path to the GTFS feed zip file

        Raises:
            FileNotFoundError: If the feed path does not exist
            ValueError: If the feed is not a valid zip file
        """
        self.feed_path = feed_path
        self.tables: dict[str, list[dict[str, Any]]] = {}

        if not os.path.exists(feed_path):
            raise FileNotFoundError(f"GTFS feed not found at {feed_path}")

        if not zipfile.is_zipfile(feed_path):
            raise ValueError(f"GTFS feed at {feed_path} is not a valid zip file")

        self._required_files = [
            "agency.txt",
            "stops.txt",
            "routes.txt",
            "trips.txt",
            "stop_times.txt",
        ]

        self._optional_files = [
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

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate that the GTFS feed has all required files.

        Returns:
            A tuple containing a boolean indicating if the feed is valid,
            and a list of error messages.
        """
        errors = []

        try:
            with zipfile.ZipFile(self.feed_path, "r") as zip_ref:
                file_list = zip_ref.namelist()

                # Check required files
                for req_file in self._required_files:
                    if req_file not in file_list:
                        errors.append(
                            f"Required file {req_file} not found in GTFS feed"
                        )

                # Check for non-standard files
                for file in file_list:
                    if (
                        file.endswith(".txt")
                        and file not in self._required_files
                        and file not in self._optional_files
                    ):
                        logger.warning(f"Non-standard file found in GTFS feed: {file}")

                # Sample validation of file contents
                if "stops.txt" in file_list:
                    try:
                        with zip_ref.open("stops.txt") as stops_file:
                            reader = csv.DictReader(
                                io.TextIOWrapper(stops_file, "utf-8-sig")
                            )
                            required_fields = [
                                "stop_id",
                                "stop_name",
                                "stop_lat",
                                "stop_lon",
                            ]
                            missing_fields = [
                                f for f in required_fields if f not in reader.fieldnames
                            ]
                            if missing_fields:
                                errors.append(
                                    f"Missing required fields in stops.txt: {', '.join(missing_fields)}"
                                )
                    except Exception as e:
                        errors.append(f"Error reading stops.txt: {str(e)}")

                # More validations could be added here for other files

        except Exception as e:
            errors.append(f"Error validating GTFS feed: {str(e)}")

        return len(errors) == 0, errors

    def load(self) -> dict[str, list[dict[str, Any]]]:
        """
        Load all data from the GTFS feed.

        Returns:
            Dictionary containing all GTFS tables

        Raises:
            ValueError: If the feed is invalid
        """
        is_valid, errors = self.validate()
        if not is_valid:
            error_msg = "\n".join(errors)
            raise ValueError(f"Invalid GTFS feed:\n{error_msg}")

        try:
            with zipfile.ZipFile(self.feed_path, "r") as zip_ref:
                file_list = zip_ref.namelist()

                # Load required and optional files
                all_files = self._required_files + self._optional_files
                for file in all_files:
                    if file in file_list:
                        self.tables[file[:-4]] = self._read_csv_file(zip_ref, file)

            logger.info(f"Loaded GTFS feed with {len(self.tables)} tables")
            return self.tables

        except Exception as e:
            logger.error(f"Error loading GTFS feed: {str(e)}")
            raise ValueError(f"Failed to load GTFS feed: {str(e)}")

    def _read_csv_file(
        self, zip_ref: zipfile.ZipFile, file_name: str
    ) -> list[dict[str, Any]]:
        """
        Read a CSV file from the zip archive.

        Args:
            zip_ref: Open ZipFile reference
            file_name: Name of the file to read

        Returns:
            List of dictionaries representing the CSV data
        """
        data = []
        try:
            with zip_ref.open(file_name) as file:
                # Use TextIOWrapper to handle text encoding properly
                text_file = io.TextIOWrapper(file, "utf-8-sig")
                reader = csv.DictReader(text_file)

                for row in reader:
                    # Clean the row data
                    cleaned_row = {k: self._clean_value(k, v) for k, v in row.items()}
                    data.append(cleaned_row)

        except Exception as e:
            logger.error(f"Error reading {file_name}: {str(e)}")
            raise

        return data

    def _clean_value(self, field: str, value: str) -> Any:
        """
        Clean and convert a value based on field name.

        Args:
            field: Field name
            value: String value from CSV

        Returns:
            Cleaned and possibly converted value
        """
        if value is None or value.strip() == "":
            return None

        # Convert lat/lon to float
        if field in ["stop_lat", "stop_lon", "shape_pt_lat", "shape_pt_lon"]:
            try:
                return float(value)
            except ValueError:
                logger.warning(f"Invalid float value in {field}: {value}")
                return None

        # Convert numeric IDs to integers if possible
        if field.endswith("_id") or field in [
            "route_type",
            "direction_id",
            "wheelchair_accessible",
            "bikes_allowed",
            "location_type",
            "wheelchair_boarding",
            "shape_pt_sequence",
            "stop_sequence",
        ]:
            try:
                return int(value)
            except ValueError:
                # If it's not a valid integer, keep it as string
                return value

        return value

    def get_table(self, table_name: str) -> list[dict[str, Any]]:
        """
        Get a specific table from the feed.

        Args:
            table_name: Name of the table (without .txt extension)

        Returns:
            List of dictionaries representing the table data

        Raises:
            KeyError: If the table doesn't exist
        """
        if not self.tables:
            self.load()

        if table_name not in self.tables:
            raise KeyError(f"Table {table_name} not found in GTFS feed")

        return self.tables[table_name]

    def get_stops(self) -> list[dict[str, Any]]:
        """
        Get all stops from the feed.

        Returns:
            List of stop dictionaries
        """
        return self.get_table("stops")

    def get_routes(self) -> list[dict[str, Any]]:
        """
        Get all routes from the feed.

        Returns:
            List of route dictionaries
        """
        return self.get_table("routes")

    def get_trips(self, route_id: str | None = None) -> list[dict[str, Any]]:
        """
        Get trips, optionally filtered by route_id.

        Args:
            route_id: Optional route ID to filter by

        Returns:
            List of trip dictionaries
        """
        trips = self.get_table("trips")

        if route_id is not None:
            return [trip for trip in trips if trip.get("route_id") == route_id]

        return trips

    def get_stop_times(self, trip_id: str | None = None) -> list[dict[str, Any]]:
        """
        Get stop times, optionally filtered by trip_id.

        Args:
            trip_id: Optional trip ID to filter by

        Returns:
            List of stop_time dictionaries
        """
        stop_times = self.get_table("stop_times")

        if trip_id is not None:
            return [st for st in stop_times if st.get("trip_id") == trip_id]

        return stop_times

    def get_shapes(self, shape_id: str | None = None) -> list[dict[str, Any]]:
        """
        Get shapes, optionally filtered by shape_id.

        Args:
            shape_id: Optional shape ID to filter by

        Returns:
            List of shape point dictionaries
        """
        try:
            shapes = self.get_table("shapes")

            if shape_id is not None:
                return [shape for shape in shapes if shape.get("shape_id") == shape_id]

            return shapes
        except KeyError:
            # Shapes are optional in GTFS
            return []

    def get_route_stops(self, route_id: str) -> list[dict[str, Any]]:
        """
        Get all stops for a specific route.

        Args:
            route_id: Route ID

        Returns:
            List of stop dictionaries in order of stop_sequence
        """
        trips = self.get_trips(route_id)
        if not trips:
            return []

        # Use the first trip as representative
        trip = trips[0]
        trip_id = trip.get("trip_id")

        # Get stop times for this trip
        stop_times = self.get_stop_times(trip_id)
        stop_times.sort(key=lambda x: x.get("stop_sequence", 0))

        # Get stop details
        stops = self.get_stops()
        stop_dict = {stop.get("stop_id"): stop for stop in stops}

        # Combine stop times with stop details
        result = []
        for stop_time in stop_times:
            stop_id = stop_time.get("stop_id")
            if stop_id in stop_dict:
                stop_info = stop_dict[stop_id].copy()
                stop_info.update(
                    {
                        "arrival_time": stop_time.get("arrival_time"),
                        "departure_time": stop_time.get("departure_time"),
                        "stop_sequence": stop_time.get("stop_sequence"),
                        "pickup_type": stop_time.get("pickup_type"),
                        "drop_off_type": stop_time.get("drop_off_type"),
                    }
                )
                result.append(stop_info)

        return result

    def get_route_shape(self, route_id: str) -> list[dict[str, Any]]:
        """
        Get the shape for a specific route.

        Args:
            route_id: Route ID

        Returns:
            List of shape point dictionaries in order of shape_pt_sequence
        """
        trips = self.get_trips(route_id)
        if not trips:
            return []

        # Find a trip with a shape_id
        shape_id = None
        for trip in trips:
            if trip.get("shape_id"):
                shape_id = trip.get("shape_id")
                break

        if not shape_id:
            return []

        # Get shape points for this shape_id
        shape_points = self.get_shapes(shape_id)
        shape_points.sort(key=lambda x: x.get("shape_pt_sequence", 0))

        return shape_points

    def get_route_types(self) -> dict[int, str]:
        """
        Get mapping of route types to their descriptions.

        Returns:
            Dictionary mapping route_type values to descriptions
        """
        return {
            0: "Tram, Streetcar, Light rail",
            1: "Subway, Metro",
            2: "Rail",
            3: "Bus",
            4: "Ferry",
            5: "Cable tram",
            6: "Aerial lifeway",
            7: "Funicular",
            11: "Trolleybus",
            12: "Monorail",
        }

    def get_calendar_dates(self, service_id: str | None = None) -> list[dict[str, Any]]:
        """
        Get calendar dates, optionally filtered by service_id.

        Args:
            service_id: Optional service ID to filter by

        Returns:
            List of calendar_date dictionaries
        """
        try:
            calendar_dates = self.get_table("calendar_dates")

            if service_id is not None:
                return [
                    cd for cd in calendar_dates if cd.get("service_id") == service_id
                ]

            return calendar_dates
        except KeyError:
            # calendar_dates.txt is optional
            return []

    def get_active_services(self, date: datetime | None = None) -> list[str]:
        """
        Get service IDs that are active on a specific date.

        Args:
            date: Date to check (defaults to today)

        Returns:
            List of active service IDs
        """
        if date is None:
            date = datetime.now()

        date_str = date.strftime("%Y%m%d")
        day_of_week = date.strftime("%A").lower()

        active_services = []

        # Check calendar.txt if it exists
        try:
            calendar = self.get_table("calendar")
            for service in calendar:
                start_date = service.get("start_date")
                end_date = service.get("end_date")

                # Skip if date is outside service range
                if (start_date and date_str < start_date) or (
                    end_date and date_str > end_date
                ):
                    continue

                # Check if this day of week is active
                if service.get(day_of_week) == "1":
                    active_services.append(service.get("service_id"))
        except KeyError:
            # calendar.txt is optional
            pass

        # Check calendar_dates.txt for exceptions
        try:
            calendar_dates = self.get_table("calendar_dates")
            for entry in calendar_dates:
                if entry.get("date") == date_str:
                    service_id = entry.get("service_id")
                    exception_type = entry.get("exception_type")

                    # exception_type=1 means service added, exception_type=2 means service removed
                    if exception_type == "1" and service_id not in active_services:
                        active_services.append(service_id)
                    elif exception_type == "2" and service_id in active_services:
                        active_services.remove(service_id)
        except KeyError:
            # calendar_dates.txt is optional
            pass

        return active_services


class GTFSToOSMMapper:
    """Class for mapping GTFS data to OSM schema."""

    @staticmethod
    def map_route_type_to_osm(gtfs_route_type: int | str) -> dict[str, str]:
        """
        Map GTFS route_type to OSM route tags.

        Args:
            gtfs_route_type: GTFS route_type value

        Returns:
            Dictionary with OSM tags
        """
        # Convert to int if it's a string
        try:
            route_type = int(gtfs_route_type)
        except (ValueError, TypeError):
            route_type = 3  # Default to bus if conversion fails

        # Basic mapping of GTFS route types to OSM route values
        route_type_map = {
            0: {"route": "tram", "railway": "tram"},
            1: {"route": "subway", "railway": "subway"},
            2: {"route": "train", "railway": "train"},
            3: {"route": "bus", "highway": "bus_stop"},
            4: {"route": "ferry", "route_master": "ferry"},
            5: {"route": "trolleybus", "highway": "bus_stop"},
            6: {"route": "aerialway", "aerialway": "cable_car"},
            7: {"route": "funicular", "railway": "funicular"},
            11: {"route": "trolleybus", "highway": "bus_stop"},
            12: {"route": "monorail", "railway": "monorail"},
        }

        return route_type_map.get(route_type, {"route": "bus", "highway": "bus_stop"})

    @staticmethod
    def map_stop_to_osm(stop: dict[str, Any]) -> dict[str, str]:
        """
        Map GTFS stop to OSM tags.

        Args:
            stop: GTFS stop dictionary

        Returns:
            Dictionary with OSM tags
        """
        osm_tags = {
            "name": stop.get("stop_name", ""),
            "ref": stop.get("stop_code", ""),
            "gtfs:stop_id": str(stop.get("stop_id", "")),
        }

        # Add public_transport tag based on location_type
        location_type = stop.get("location_type", 0)
        if location_type == 0:
            osm_tags["public_transport"] = "platform"
        elif location_type == 1:
            osm_tags["public_transport"] = "stop_area"

        # Add wheelchair tag if available
        wheelchair_boarding = stop.get("wheelchair_boarding")
        if wheelchair_boarding == 1:
            osm_tags["wheelchair"] = "yes"
        elif wheelchair_boarding == 2:
            osm_tags["wheelchair"] = "no"

        return osm_tags

    @staticmethod
    def map_route_to_osm(
        route: dict[str, Any], agency_name: str | None = None
    ) -> dict[str, str]:
        """
        Map GTFS route to OSM tags.

        Args:
            route: GTFS route dictionary
            agency_name: Optional agency name

        Returns:
            Dictionary with OSM tags
        """
        route_type = route.get("route_type", 3)  # Default to bus
        osm_route_tags = GTFSToOSMMapper.map_route_type_to_osm(route_type)

        # Basic route tags
        osm_tags = {
            "type": "route",
            "route": osm_route_tags.get("route", "bus"),
            "ref": route.get("route_short_name", ""),
            "name": route.get("route_long_name", ""),
            "gtfs:route_id": str(route.get("route_id", "")),
        }

        # Add color if available
        if route.get("route_color"):
            osm_tags["colour"] = "#" + route["route_color"]

        # Add network tag if agency is available
        if agency_name:
            osm_tags["network"] = agency_name

        # Add frequency information if available
        if route.get("headway_secs"):
            try:
                headway_mins = int(int(route["headway_secs"]) / 60)
                osm_tags["interval"] = str(headway_mins)
            except (ValueError, TypeError):
                pass

        return osm_tags


class GTFSReader:
    """Utility class for reading GTFS feeds."""

    @staticmethod
    def read_feed(feed_path: str) -> GTFSFeed:
        """
        Create and load a GTFS feed.

        Args:
            feed_path: Path to the GTFS feed zip file

        Returns:
            Loaded GTFSFeed object
        """
        feed = GTFSFeed(feed_path)
        feed.load()
        return feed

    @staticmethod
    def get_agency_name(feed: GTFSFeed, agency_id: str | None = None) -> str | None:
        """
        Get the name of an agency from the feed.

        Args:
            feed: GTFSFeed object
            agency_id: Optional agency ID to look up

        Returns:
            Agency name or None if not found
        """
        try:
            agencies = feed.get_table("agency")

            if not agencies:
                return None

            if agency_id is not None:
                for agency in agencies:
                    if agency.get("agency_id") == agency_id:
                        return agency.get("agency_name")
                return None
            else:
                # If no agency_id specified, return the first agency name
                return agencies[0].get("agency_name")

        except KeyError:
            return None
