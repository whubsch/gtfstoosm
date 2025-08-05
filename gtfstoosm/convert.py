"""
Core module for converting GTFS feeds to OSM relations.

This module contains the main functionality for reading GTFS data and
converting it to OSM relations that can be imported into OpenStreetMap.
"""

import logging
from typing import Any, cast
import requests
import time
import atlus
import polars as pl
import random

from gtfstoosm.osm import OSMElement, OSMNode, OSMRelation
from gtfstoosm.utils import string_to_unique_int, deduplicate_lists
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
        self.relations: list[OSMRelation] = []
        self.nodes: list[OSMNode] = []

    def __str__(self) -> str:
        return f"OSMRelationBuilder(include_stops={self.include_stops}, include_routes={self.include_routes}, route_types={self.route_types}, agency_id={self.agency_id})"

    def build_relations(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
        """
        Build OSM relations from GTFS data.

        Args:
            gtfs_data: The GTFS data as returned by GTFSFeed.load()
        """
        logger.info("Building OSM relations from GTFS data")

        # if self.include_stops:
        #     self._process_stops(gtfs_data["stops"])

        if self.include_routes:
            self._process_routes(gtfs_data, include_stops=self.include_stops)

        logger.info(
            f"Built {len(self.relations)} route relations and {len(self.nodes)} nodes"
        )

    def build_route_masters(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
        """
        Create route_master relations for routes with the same ref.
        """
        logger.info("Creating route_master relations")

        made_routes = set(variant.tags["ref"] for variant in self.relations)
        unique_routes = gtfs_data["routes"].filter(
            pl.col("route_id").is_in(made_routes)
        )

        for unique_route in unique_routes.iter_rows():
            master = OSMRelation(
                id=-1 * random.randint(1, 10**6),
                tags={
                    "type": "route_master",
                    "route_master": "bus",
                    "ref": unique_route[0],
                    "name": f"WMATA {unique_route[0]} "
                    + atlus.abbrs(atlus.get_title(unique_route[3], single_word=True)),
                    "colour": "#" + unique_route[7],
                },
            )
            for route in self.relations:
                if route.tags.get("ref") == unique_route[0]:
                    master.add_member(osm_type="relation", ref=route.id)
            self.relations.append(master)
        logger.info(
            f"Built {len([i for i in self.relations if i.tags.get('route_master')])} route_master relations"
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

    def _process_routes(
        self, gtfs_data: dict[str, pl.DataFrame], include_stops: bool
    ) -> None:
        """
        Process GTFS routes and convert them to OSM relations.

        Args:
            gtfs_data: Complete GTFS data dictionary
            include_stops: Whether to include stops in the output relations
        """
        routes = gtfs_data["routes"]
        trips = gtfs_data["trips"]
        stop_times = gtfs_data["stop_times"]
        stops = gtfs_data["stops"]

        logger.info(f"Processing {routes.height} routes")

        # Filter routes by type if specified
        if self.route_types:
            routes = routes.filter(pl.col("route_type").is_in(self.route_types))

        # Only process routes that start with 'P' for now
        routes_to_process = routes.filter(pl.col("route_id").str.starts_with("P9"))

        logger.info(f"Processing {routes_to_process.height} filtered routes")

        # Group trips by route_id
        route_trips_grouped = trips.group_by("route_id", maintain_order=True)

        # Process each route
        for route_id, route_trips in route_trips_grouped:
            route_ref: str = cast(str, route_id[0])

            try:
                # Get route information
                route_info = routes_to_process.filter(
                    pl.col("route_id") == route_ref
                ).row(0)
                logger.info(f"Processing route {route_ref}")

            except pl.exceptions.OutOfBoundsError:
                continue

            # Get all trip_ids for this route
            trip_ids: list[int] = route_trips["trip_id"].to_list()

            # Get all stop sequences for these trips
            trip_stop_times = stop_times.filter(pl.col("trip_id").is_in(trip_ids))

            # Group stop times by trip_id and sort by stop_sequence
            trip_sequences = []
            for trip_id in trip_ids:
                trip_stops = trip_stop_times.filter(pl.col("trip_id") == trip_id).sort(
                    "stop_sequence"
                )
                stop_ids = trip_stops["stop_id"].to_list()
                trip_sequences.append(stop_ids)

            # Deduplicate stop sets
            trip_sequences = deduplicate_lists(trip_sequences)

            # Process each unique stop pattern
            for trip_sequence in trip_sequences:
                # Get the stop locations (with lat and long)
                stop_locations = self._get_stop_locations(trip_sequence, stops)
                stop_objects = self._get_stop_objects(stop_locations)

                # Get the OSM ways that make up the route
                osm_way_ids = self._get_route_ways(stop_locations)
                logger.info(
                    f"Found {len(osm_way_ids)} total ways for route {route_ref} variant"
                )

                # Create OSM relation
                relation = OSMRelation(
                    id=-1 * random.randint(1, string_to_unique_int(route_ref)),
                    tags={
                        "type": "route",
                        "public_transport:version": "2",
                        "route": self._get_osm_route_type(route_info[5]),
                        "ref": route_info[2],
                        "name": f"WMATA {route_info[2]} "
                        + atlus.abbrs(atlus.get_title(route_info[3], single_word=True)),
                        "colour": "#" + route_info[7],
                    },
                    members=[],
                )

                # Add stop objects as members
                for stop_object in stop_objects:
                    relation.add_member(
                        osm_type="node", ref=stop_object.id, role="platform"
                    )

                # Add ways as members
                for way_id in osm_way_ids:
                    relation.add_member(osm_type="way", ref=way_id)

                self.relations.append(relation)

    def _get_stop_objects(self, stops: pl.DataFrame) -> list[OSMElement]:
        """
        Get OSM elements for stops by querying nearby bus stops from OpenStreetMap.

        Args:
            stops: DataFrame containing stop information with lat, lon, name, and stop_id columns

        Returns:
            List of OSMElement objects representing nearby bus stops, ordered to match input stops
        """
        if stops.is_empty():
            return []

        osm_elements = []
        overpass_url = "https://overpass-api.de/api/interpreter"

        # Build a single query with all coordinates
        around_clauses = [
            f"(around:5,{stop_row['lat']},{stop_row['lon']})"
            for stop_row in stops.iter_rows(named=True)
        ]

        query_meat = "\n".join(
            f"""
        node["highway"="bus_stop"]{around};
        node["public_transport"="platform"]{around};
        """
            for around in around_clauses
        )

        query = f"""
        [out:json][timeout:60];
        (
        {query_meat}
        );
        out geom;
        """

        max_retries = 3
        retry_count = 0

        while retry_count <= max_retries:
            try:
                logger.info(
                    f"Querying Overpass API for {stops.height} stop locations (attempt {retry_count + 1})"
                )
                response = requests.post(
                    overpass_url,
                    data=query,
                    headers={
                        "User-Agent": "gtfstoosm (https://github.com/whubsch/gtfstoosm)"
                    },
                )

                if response.status_code == 200:
                    result = response.json()

                    # Create OSMNode objects from all results
                    all_osm_nodes = []
                    for element in result.get("elements", []):
                        if element["type"] == "node":
                            osm_node = OSMNode(
                                id=element["id"],
                                lat=element["lat"],
                                lon=element["lon"],
                                tags=element.get("tags", {}),
                            )
                            all_osm_nodes.append(osm_node)

                    # Now match each input stop to its nearest OSM node
                    for stop_row in stops.iter_rows(named=True):
                        stop_lat = stop_row["lat"]
                        stop_lon = stop_row["lon"]

                        # Find the closest OSM node to this stop
                        closest_node = None
                        min_distance = float("inf")

                        for osm_node in all_osm_nodes:
                            # Calculate distance using Haversine formula (approximate)
                            distance = self._calculate_distance(
                                stop_lat, stop_lon, osm_node.lat, osm_node.lon
                            )

                            if (
                                distance < min_distance and distance <= 5
                            ):  # Within 5 meter radius
                                min_distance = distance
                                closest_node = osm_node

                        # Add the closest node (or None if no match within 5m)
                        if closest_node:
                            osm_elements.append(closest_node)
                            # Remove from pool to avoid duplicate matches
                            all_osm_nodes.remove(closest_node)
                        else:
                            logger.debug(
                                f"No OSM stop found within 5m of GTFS stop at {stop_lat}, {stop_lon}"
                            )

                    # Success - break out of retry loop
                    break

                elif response.status_code == 429:
                    retry_count += 1
                    if retry_count <= max_retries:
                        logger.warning(
                            f"Rate limited by Overpass API (HTTP 429). Retrying in 2 seconds... (attempt {retry_count + 1}/{max_retries + 1})"
                        )
                        time.sleep(2)
                    else:
                        logger.error(
                            f"Rate limited by Overpass API (HTTP 429). Max retries ({max_retries}) exceeded."
                        )
                        break
                else:
                    logger.warning(
                        f"Failed to query Overpass API: HTTP {response.status_code}"
                    )
                    break

            except Exception as e:
                logger.warning(f"Error querying nearby stops: {str(e)}")
                break

        logger.info(f"Found {len(osm_elements)} OSM stop elements (ordered)")
        return osm_elements

    def _calculate_distance(
        self, lat1: float, lon1: float, lat2: float, lon2: float
    ) -> float:
        """
        Calculate the distance between two points using Haversine formula.

        Returns:
            Distance in meters
        """
        import math

        # Convert latitude and longitude from degrees to radians
        lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

        # Haversine formula
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.asin(math.sqrt(a))

        # Radius of earth in meters
        r = 6371000

        return c * r

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
        self,
        stop_locations: pl.DataFrame,
        costing: str = "bus",
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> list[int]:
        """
        Get OSM way IDs for a route between stops using Valhalla API.

        Args:
            stop_locations: List of dictionaries containing stop information with lat and lon
            costing: Valhalla costing model to use (bus, auto, pedestrian, etc.)
            max_retries: Maximum number of retry attempts if request fails
            retry_delay: Delay in seconds between retry attempts

        Returns:
            List of unique OSM way IDs that make up the route
        """
        logger.info(f"Getting OSM ways for route with {stop_locations.height} stops")

        route_ways = []

        # Get the input data for the request
        valhalla_url = "https://valhalla1.openstreetmap.de/trace_attributes"
        lats = stop_locations["lat"].to_list()
        lons = stop_locations["lon"].to_list()

        request_json = {
            "shape": [{"lat": lat, "lon": lon} for lat, lon in zip(lats, lons)],
            "costing": costing,
            "costing_options": {
                "maneuver_penalty": 10,
                "include_hov2": True,
                "include_hov3": True,
            },
            "format": "osrm",
            "shape_match": "map_snap",
            "filters": {"attributes": ["edge.way_id", "edge.names"]},
        }

        # Try the request with retries
        retry_count = 0
        while retry_count <= max_retries:
            try:
                response = requests.post(
                    valhalla_url,
                    json=request_json,
                    headers={
                        "User-Agent": "gtfstoosm (https://github.com/whubsch/gtfstoosm)"
                    },
                ).json()

                # Check if the response contains the expected data
                if "edges" in response:
                    # Extract the way IDs
                    for edge in response.get("edges", []):
                        if "way_id" in edge:
                            way_id = edge["way_id"]
                            if way_id not in route_ways:
                                route_ways.append(way_id)
                    return route_ways
                else:
                    logger.warning(
                        f"Invalid response from Valhalla (attempt {retry_count + 1}/{max_retries + 1})"
                    )

            except Exception as e:
                logger.warning(
                    f"Error getting ways between stops (attempt {retry_count + 1}/{max_retries + 1}): {str(e)}"
                )

            # If we get here, the request failed or the response was invalid
            if retry_count < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                import time

                time.sleep(retry_delay)
                retry_count += 1
            else:
                logger.error(f"Failed to get ways after {max_retries + 1} attempts")
                break

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

        # Writing logic
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
        builder.build_route_masters(loader.tables)

        # Write to file
        builder.write_to_file(osm_path)

        return True

    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        raise
