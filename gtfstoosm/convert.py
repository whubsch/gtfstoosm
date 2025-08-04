"""
Core module for converting GTFS feeds to OSM relations.

This module contains the main functionality for reading GTFS data and
converting it to OSM relations that can be imported into OpenStreetMap.
"""

import logging
from typing import Any
import requests
import atlus
import polars as pl

from gtfstoosm.osm import OSMRelation
from gtfstoosm.utils import string_to_unique_int
from gtfstoosm.gtfs import GTFSFeed

logger = logging.getLogger(__name__)


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

    def build_relations(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
        """
        Build OSM relations from GTFS data.

        Args:
            gtfs_data: The GTFS data as returned by GTFSFeed.load()
        """
        logger.info("Building OSM relations from GTFS data")

        if self.include_stops:
            self._process_stops(gtfs_data["stops"])

        if self.include_routes:
            self._process_routes(gtfs_data)

        logger.info(
            f"Built {len(self.relations)} relations and {len(self.nodes)} nodes"
        )

    def _process_stops(self, stops: pl.DataFrame) -> None:
        """
        Process GTFS stops and convert them to OSM nodes.

        Args:
            stops: List of GTFS stop dictionaries
        """
        logger.info(f"Processing {len(stops)} stops")

        # Placeholder for stop processing logic
        raise NotImplementedError("Stop processing not implemented yet")

    def _process_routes(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
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
            routes = routes.filter(pl.col("route_type").is_in(self.route_types))

        print(routes)
        # Placeholder for route processing logic
        for route in routes.iter_rows():
            if route[0].startswith("F8"):
                pass
            else:
                continue
            # Get representative trip for this route
            # route_trips = [t for t in trips if t["route_id"] == route["route_id"]]
            route_trips = trips.filter(pl.col("route_id") == route[0])
            if route_trips.is_empty():
                continue

            # Use the first trip as representative
            trip = route_trips[0]

            # Get stop sequence for this trip
            # trip_stops = [st for st in stop_times if st["trip_id"] == trip["trip_id"]]
            trip_stops = stop_times.filter(pl.col("trip_id") == trip[0])
            trip_stops.sort("stop_sequence")

            # Get the stop IDs from the stop_times
            # stop_ids: Sequence[Any] = [st.get("stop_id") for st in trip_stops]
            stop_ids = trip_stops["stop_id"].to_list()

            # Get the stop locations (with lat and long)
            stop_locations = self._get_stop_locations(stop_ids, stops)

            # Get the OSM ways that make up the route
            osm_way_ids = self._get_route_ways(stop_locations)
            logger.info(f"Found {len(osm_way_ids)} total ways for route {route[0]}")

            # Create OSM relation
            relation = OSMRelation(
                **{
                    "id": f"-{string_to_unique_int(route[0])}",
                    "type": "route",
                    "tags": {
                        "type": "route",
                        "public_transport:version": "2",
                        "route": self._get_osm_route_type(route[5]),
                        "ref": route[2],
                        "name": atlus.abbrs(route[3]),
                        # "network": self._get_network_name(route, gtfs_data["agency"]),
                    },
                    "members": [],
                }
            )

            # Add ways as members
            for way_id in osm_way_ids:
                relation.add_member(**{"osm_type": "way", "ref": way_id, "role": ""})

            self.relations.append(relation)

    def _get_stop_locations(
        self, stop_ids: list[str], stops: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Get location information for a list of stop IDs.

        Args:
            stop_ids: List of stop IDs to get locations for
            stops: Complete list of GTFS stops data

        Returns:
            List of dictionaries containing stop information with lat and lon
        """
        # Create a DataFrame from stop_ids to preserve order
        stop_ids_df = pl.DataFrame({"stop_id": stop_ids, "order": range(len(stop_ids))})

        # Join with stops data and sort by original order
        stop_locations = (
            stop_ids_df.join(
                stops.select(["stop_id", "stop_lat", "stop_lon", "stop_name"]),
                on="stop_id",
                how="inner",
            )
            .sort("order")
            .select(
                [
                    "stop_id",
                    pl.col("stop_lat").alias("lat"),
                    pl.col("stop_lon").alias("lon"),
                    pl.col("stop_name").alias("name"),
                ]
            )
        )

        return stop_locations

    def _get_route_ways(
        self, stop_locations: pl.DataFrame, costing: str = "bus"
    ) -> list[int]:
        """
        Get OSM way IDs for a route between stops using Valhalla API.

        Args:
            stop_locations: List of dictionaries containing stop information with lat and lon
            costing: Valhalla costing model to use (bus, auto, pedestrian, etc.)

        Returns:
            List of unique OSM way IDs that make up the route
        """
        logger.info(f"Getting OSM ways for route with {stop_locations.height} stops")

        route_ways = []

        try:
            # Get the ways between these stops
            valhalla_url = "https://valhalla1.openstreetmap.de/trace_attributes"
            lats = stop_locations["lat"].to_list()
            lons = stop_locations["lon"].to_list()

            request_json = {
                "shape": [{"lat": lat, "lon": lon} for lat, lon in zip(lats, lons)],
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
        loader = GTFSFeed(feed_dir=gtfs_path)
        loader.load()

        # Build OSM relations
        builder = OSMRelationBuilder(
            # include_stops=options.get("include_stops", False),
            include_stops=False,
            include_routes=options.get("include_routes", True),
            route_types=options.get("route_types"),
            agency_id=options.get("agency_id"),
        )
        builder.build_relations(loader.tables)

        # Write to file
        builder.write_to_file(osm_path)

        return True

    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        raise
