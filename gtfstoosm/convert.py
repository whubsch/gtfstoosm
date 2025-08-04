"""
Core module for converting GTFS feeds to OSM relations.

This module contains the main functionality for reading GTFS data and
converting it to OSM relations that can be imported into OpenStreetMap.
"""

import os
import logging
import zipfile
from typing import Any, Sequence
import polars as pl
from io import BytesIO
import requests
import atlus

from gtfstoosm.osm import OSMRelation
from gtfstoosm.utils import string_to_unique_int

logger = logging.getLogger(__name__)


class GTFSFeedLoader:
    """Class for loading and parsing GTFS feed data."""

    def __init__(self, feed_path: str):
        """
        Initialize the GTFS feed loader.

        Args:
            feed_path: Path to the GTFS feed zip file
        """
        self.feed_path = feed_path
        if not os.path.exists(feed_path):
            raise FileNotFoundError(f"GTFS feed not found at {feed_path}")

    def load(self) -> dict[str, list[dict[str, Any]]]:
        """
        Load the GTFS feed data.

        Returns:
            A dictionary containing the parsed GTFS data with keys for
            'routes', 'stops', 'trips', 'stop_times', etc.

        Raises:
            ValueError: If the GTFS feed is invalid
        """
        logger.info(f"Loading GTFS feed from {self.feed_path}")

        try:
            data = {
                "routes": self._load_table("routes.txt"),
                "stops": self._load_table("stops.txt"),
                "trips": self._load_table("trips.txt"),
                "stop_times": self._load_table("stop_times.txt"),
                "agency": self._load_table("agency.txt", required=False),
            }

            logger.info(
                f"Loaded GTFS feed with {len(data['routes'])} routes and {len(data['stops'])} stops"
            )
            return data

        except Exception as e:
            logger.error(f"Error loading GTFS feed: {str(e)}")
            raise ValueError(f"Invalid GTFS feed: {str(e)}")

    def _load_table(self, filename: str, required: bool = True) -> list[dict[str, Any]]:
        """
        Load a specific table from the GTFS feed.

        Args:
            filename: The name of the file to load (e.g., 'routes.txt')
            required: Whether this file is required. If True and the file
                        is missing, an exception will be raised.

        Returns:
            A list of dictionaries, where each dictionary represents a row
            in the table with column names as keys.

        Raises:
            ValueError: If a required file is missing or invalid
        """
        try:
            with zipfile.ZipFile(self.feed_path, "r") as zip_ref:
                if filename not in zip_ref.namelist():
                    if required:
                        raise ValueError(
                            f"Required file {filename} not found in GTFS feed"
                        )
                    else:
                        logger.warning(
                            f"Optional file {filename} not found in GTFS feed"
                        )
                        return []

                with zip_ref.open(filename) as file:
                    # Read the CSV data into a polars DataFrame
                    df = pl.read_csv(BytesIO(file.read()), infer_schema_length=None)

                    # Convert to list of dictionaries
                    records = df.to_dicts()
                    logger.info(f"Loaded {len(records):,} records from {filename}")
                    return records

        except zipfile.BadZipFile:
            raise ValueError(f"The file at {self.feed_path} is not a valid zip file")
        except Exception as e:
            raise ValueError(f"Error loading {filename}: {str(e)}")


class OSMRelationBuilder:
    """Class for building OSM relations from GTFS data."""

    def __init__(
        self,
        include_stops: bool = False,
        include_routes: bool = True,
        route_types: list[int] | None = None,
        agency_id: str | None = None,
    ):
        """
        Initialize the OSM relation builder.

        Args:
            include_stops: Whether to include stops in the output
            include_routes: Whether to include routes in the output
            route_types: List of route types to include (GTFS route_type values)
            agency_id: Only include routes for this agency
        """
        self.include_stops = include_stops
        self.include_routes = include_routes
        self.route_types = route_types
        self.agency_id = agency_id
        self.relations = []
        self.nodes = []

    def __str__(self) -> str:
        return f"OSMRelationBuilder(include_stops={self.include_stops}, include_routes={self.include_routes}, route_types={self.route_types}, agency_id={self.agency_id})"

    def build_relations(self, gtfs_data: dict[str, list[dict[str, Any]]]) -> None:
        """
        Build OSM relations from GTFS data.

        Args:
            gtfs_data: The GTFS data as returned by GTFSFeedLoader.load()
        """
        logger.info("Building OSM relations from GTFS data")

        if self.include_stops:
            self._process_stops(gtfs_data["stops"])

        if self.include_routes:
            self._process_routes(gtfs_data)

        logger.info(
            f"Built {len(self.relations)} relations and {len(self.nodes)} nodes"
        )

    def _process_stops(self, stops: list[dict[str, Any]]) -> None:
        """
        Process GTFS stops and convert them to OSM nodes.

        Args:
            stops: List of GTFS stop dictionaries
        """
        logger.info(f"Processing {len(stops)} stops")

        for stop in stops:
            # Convert GTFS stop to OSM node
            node = {
                "id": stop["stop_id"],
                "lat": stop["stop_lat"],
                "lon": stop["stop_lon"],
                "tags": {
                    "name": stop["stop_name"],
                    "public_transport": "platform",
                    "highway": "bus_stop",  # This would be determined by route_type in reality
                    "gtfs:stop_id": stop["stop_id"],
                },
            }
            self.nodes.append(node)

    def _process_routes(self, gtfs_data: dict[str, list[dict[str, Any]]]) -> None:
        """
        Process GTFS routes and convert them to OSM relations.

        Args:
            gtfs_data: Complete GTFS data dictionary
        """
        routes = gtfs_data["routes"]
        trips = gtfs_data["trips"]
        stop_times = gtfs_data["stop_times"]
        stops = gtfs_data["stops"]

        logger.info(f"Processing {len(routes)} routes")

        # Filter routes by type if specified
        if self.route_types:
            routes = [r for r in routes if r.get("route_type") in self.route_types]

        # Filter routes by agency if specified
        if self.agency_id:
            routes = [r for r in routes if r.get("agency_id") == self.agency_id]

        # Placeholder for route processing logic
        for route in routes:
            if route["route_id"].startswith("F8"):
                pass
            else:
                continue
            # Get representative trip for this route
            route_trips = [t for t in trips if t["route_id"] == route["route_id"]]
            if not route_trips:
                continue

            # Use the first trip as representative
            trip = route_trips[0]

            # Get stop sequence for this trip
            trip_stops = [st for st in stop_times if st["trip_id"] == trip["trip_id"]]
            trip_stops.sort(key=lambda x: x["stop_sequence"])

            # Get the stop IDs from the stop_times
            stop_ids: Sequence[Any] = [st.get("stop_id") for st in trip_stops]

            # Get the stop locations (with lat and long)
            stop_locations = self._get_stop_locations(stop_ids, stops)

            # Get the OSM ways that make up the route
            osm_way_ids = self._get_route_ways(stop_locations)
            logger.info(
                f"Found {len(osm_way_ids)} total ways for route {route['route_id']}"
            )

            # Create OSM relation
            relation = OSMRelation(
                **{
                    "id": f"-{string_to_unique_int(route['route_id'])}",
                    "type": "route",
                    "tags": {
                        "type": "route",
                        "public_transport:version": "2",
                        "route": self._get_osm_route_type(route["route_type"]),
                        "ref": route.get("route_short_name", ""),
                        "name": atlus.abbrs(route.get("route_long_name", "")),
                        "network": self._get_network_name(route, gtfs_data["agency"]),
                    },
                    "members": [],
                }
            )

            # Add ways as members
            for way_id in osm_way_ids:
                relation.add_member(**{"osm_type": "way", "ref": way_id, "role": ""})

            self.relations.append(relation)

    def _get_stop_locations(
        self, stop_ids: list[str], stops: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Get location information for a list of stop IDs.

        Args:
            stop_ids: List of stop IDs to get locations for
            stops: Complete list of GTFS stops data

        Returns:
            List of dictionaries containing stop information with lat and lon
        """
        stop_locations = []
        for stop_id in stop_ids:
            # Find the stop with matching stop_id
            for stop in stops:
                if stop["stop_id"] == stop_id:
                    stop_locations.append(
                        {
                            "stop_id": stop_id,
                            "lat": stop["stop_lat"],
                            "lon": stop["stop_lon"],
                            "name": stop["stop_name"],
                        }
                    )
                    break

        return stop_locations

    def _get_route_ways(
        self, stop_locations: list[dict[str, Any]], costing: str = "bus"
    ) -> list[int]:
        """
        Get OSM way IDs for a route between stops using Valhalla API.

        Args:
            stop_locations: List of dictionaries containing stop information with lat and lon
            costing: Valhalla costing model to use (bus, auto, pedestrian, etc.)

        Returns:
            List of unique OSM way IDs that make up the route
        """
        logger.info(f"Getting OSM ways for route with {len(stop_locations)} stops")

        route_ways = []

        try:
            # Get the ways between these stops
            valhalla_url = "https://valhalla1.openstreetmap.de/trace_attributes"

            request_json = {
                "shape": [
                    {"lat": stop["lat"], "lon": stop["lon"]} for stop in stop_locations
                ],
                "costing": costing,
                "format": "osrm",
                "shape_match": "map_snap",
                "filters": {"attributes": ["edge.way_id", "edge.names"]},
            }

            response = requests.post(valhalla_url, json=request_json).json()

            # Extract the way IDs
            for edge in response.get("edges", []):
                if "way_id" in edge:
                    way_id = edge["way_id"]
                    if way_id not in route_ways:
                        route_ways.append(way_id)

        except Exception as e:
            logger.warning(f"Error getting ways between stops: {str(e)}")

        return route_ways

    def _get_osm_route_type(self, gtfs_route_type: int | str) -> str:
        """
        Convert GTFS route_type to OSM route tag value.

        Args:
            gtfs_route_type: GTFS route_type value

        Returns:
            Corresponding OSM route tag value
        """
        # Convert to int if it's a string
        try:
            route_type = int(gtfs_route_type)
        except (ValueError, TypeError):
            return "bus"  # Default to bus if conversion fails

        # GTFS route types mapping to OSM route values
        route_type_map = {
            0: "tram",
            1: "subway",
            2: "train",
            3: "bus",
            4: "ferry",
            5: "trolleybus",
            6: "cable_car",
            7: "gondola",
            11: "trolleybus",
            12: "monorail",
        }

        return route_type_map.get(route_type, "bus")

    def _get_network_name(
        self, route: dict[str, Any], agencies: list[dict[str, Any]]
    ) -> str:
        """
        Get the network name for a route.

        Args:
            route: GTFS route dictionary
            agencies: List of GTFS agency dictionaries

        Returns:
            Network name for OSM
        """
        agency_id = route.get("agency_id")
        if agency_id and agencies:
            for agency in agencies:
                if agency.get("agency_id") == agency_id:
                    return agency.get("agency_name", "")

        # Default if no agency found
        return ""

    def write_to_file(self, output_path: str) -> None:
        """
        Write the OSM data to a file.

        Args:
            output_path: Path to write the OSM XML file
        """
        logger.info(f"Writing OSM data to {output_path}")

        # Placeholder for file writing logic
        # In a real implementation, this would create a proper OSM XML file
        try:
            with open(output_path, "w") as f:
                f.write("<?xml version='1.0' encoding='UTF-8'?>\n")
                f.write("<osmChange version='0.6' generator='gtfstoosm'>\n")
                f.write("<create>\n")

                # Write nodes
                for node in self.nodes:
                    f.write(node.to_xml() + "\n")

                # Write relations
                for relation in self.relations:
                    f.write(relation.to_xml() + "\n")

                f.write("</create>\n</osmChange>\n")

            logger.info(f"Successfully wrote OSM data to {output_path}")

        except Exception as e:
            logger.error(f"Error writing OSM file: {str(e)}")
            raise OSError(f"Failed to write OSM file: {str(e)}")


def convert_gtfs_to_osm(gtfs_path: str, osm_path: str, **options) -> bool:
    """
    Convert a GTFS feed to OSM relations.

    Args:
        gtfs_path: Path to the GTFS feed zip file
        osm_path: Path to write the OSM XML file
        **options: Additional options for the conversion
            - include_stops: Whether to include stops (default: True)
            - include_routes: Whether to include routes (default: True)
            - route_types: List of route types to include (default: None = all)
            - agency_id: Only include routes for this agency (default: None = all)

    Returns:
        True if conversion was successful

    Raises:
        FileNotFoundError: If the GTFS feed is not found
        ValueError: If the GTFS feed is invalid
        IOError: If writing the OSM file fails
    """
    try:
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

        # Load GTFS data
        loader = GTFSFeedLoader(gtfs_path)
        gtfs_data = loader.load()

        # Build OSM relations
        builder = OSMRelationBuilder(
            # include_stops=options.get("include_stops", False),
            include_stops=False,
            include_routes=options.get("include_routes", True),
            route_types=options.get("route_types"),
            agency_id=options.get("agency_id"),
        )
        builder.build_relations(gtfs_data)

        # Write to file
        builder.write_to_file(osm_path)

        return True

    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        raise
