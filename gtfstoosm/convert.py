"""
Core module for converting GTFS feeds to OSM relations.

This module contains the main functionality for reading GTFS data and
converting it to OSM relations that can be imported into OpenStreetMap.
"""

import logging
import random
import time

import polars as pl
import requests

from gtfstoosm.gtfs import GTFSFeed
from gtfstoosm.osm import OSMElement, OSMNode, OSMRelation
from gtfstoosm.utils import (
    Trip,
    calculate_direction,
    deduplicate_trips,
    format_name,
    string_to_unique_int,
)

logger = logging.getLogger(__name__)


class OSMRelationBuilder:
    """Class for building OSM relations from GTFS data."""

    def __init__(
        self,
        exclude_stops: bool = False,
        exclude_routes: bool = False,
        add_missing_stops: bool = False,
        route_types: list[int] | None = None,
        agency_id: str | None = None,
        search_radius: float = 10.0,
        route_direction: bool = False,
        route_ref_pattern: str | None = None,
        relation_tags: dict[str, str] | None = None,
    ):
        """
        Initialize the OSM relation builder.
        """
        self.exclude_stops = exclude_stops
        self.exclude_routes = exclude_routes
        self.add_missing_stops = add_missing_stops
        self.route_types = route_types
        self.agency_id = agency_id
        self.relations: list[OSMRelation] = []
        self.nodes: list[OSMNode] = []
        self.new_stops: list[OSMNode] = []
        self.search_radius = search_radius
        self.route_direction = route_direction
        self.route_ref_pattern = route_ref_pattern
        self.relation_tags = relation_tags

    def __str__(self) -> str:
        # Exclude None values and internal collections
        exclude_attrs = {"relations", "nodes", "new_stops"}
        attrs = [
            f"{k}={v!r}"
            for k, v in vars(self).items()
            if v is not None and not k.startswith("_") and k not in exclude_attrs
        ]
        return f"OSMRelationBuilder({', '.join(attrs)})"

    def __repr__(self) -> str:
        attrs = dict(vars(self).items())
        return (
            f"OSMRelationBuilder({', '.join(f'{k}={v!r}' for k, v in attrs.items())})"
        )

    def build_relations(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
        """
        Build OSM relations from GTFS data.

        Args:
            gtfs_data: The GTFS data as returned by GTFSFeed.load()
        """
        logger.info("Building OSM relations from GTFS data")

        self._process_routes(gtfs_data)

        logger.info(
            f"Built {len(self.relations)} route relations and {len(self.nodes) + len(self.new_stops)} nodes"
        )

    def build_route_masters(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
        """
        Create route_master relations for routes with the same ref.
        """
        logger.info("Creating route_master relations")

        made_routes = {variant.tags["ref"] for variant in self.relations}
        unique_routes = gtfs_data["routes"].filter(
            pl.col("route_id").is_in(made_routes)
        )

        for unique_route in unique_routes.iter_rows(named=True):
            route_master_tags = {
                "type": "route_master",
                "route_master": "bus",
                "ref": unique_route["route_id"],
                "name": f"Route {unique_route['route_id']} {unique_route['route_long_name']}".strip(),
            }
            if "route_color" in unique_route:
                route_master_tags["route_color"] = "#" + unique_route["route_color"]

            if self.relation_tags:
                route_master_tags.update(self.relation_tags)

            master = OSMRelation(
                id=-1 * random.randint(1, 10**6),
                tags=route_master_tags,
            )
            for route in self.relations:
                if route.tags.get("ref") == unique_route["route_id"]:
                    master.add_member(osm_type="relation", ref=route.id)
            self.relations.append(master)
        logger.info(
            f"Built {len([i for i in self.relations if i.tags.get('route_master')])} route_master relations"
        )

    def _process_routes(self, gtfs_data: dict[str, pl.DataFrame]) -> None:
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
        shapes = gtfs_data["shapes"]

        logger.info(f"Processing {routes.height} routes")

        # Filter routes by type if specified
        if self.route_types:
            routes = routes.filter(pl.col("route_type").is_in(self.route_types))

        # Filter routes by regex pattern if specified
        if self.route_ref_pattern is not None:
            routes_to_process = routes.filter(
                pl.col("route_id").cast(pl.Utf8).str.contains(self.route_ref_pattern)
            )
            logger.debug(
                f"Filtered to routes matching pattern '{self.route_ref_pattern}'"
            )
        else:
            routes_to_process = routes

        logger.info(f"Processing {routes_to_process.height} filtered routes")

        # Process routes using vectorized operations
        route_trip_stops = (
            trips.join(stop_times, on="trip_id")
            .filter(pl.col("route_id").is_in(routes_to_process["route_id"]))
            .sort(["route_id", "trip_id", "stop_sequence"])
            .group_by(["route_id", "trip_id", "shape_id"], maintain_order=True)
            .agg([pl.col("stop_id").alias("stops")])
        )

        # Process all routes at once instead of looping
        for route_data in route_trip_stops.group_by("route_id"):
            route_ref = str(route_data[0][0])  # route_id
            route_trips_data = route_data[1]  # DataFrame for this route

            try:
                route_info = routes_to_process.filter(
                    pl.col("route_id") == route_ref
                ).row(0)
                logger.info(f"Processing route {route_ref}")
            except pl.exceptions.OutOfBoundsError:
                continue

            # Create Trip objects from the grouped data
            trip_sequences = [
                Trip(
                    trip_id=row["trip_id"],
                    route_id=route_ref,
                    shape_id=row["shape_id"],
                    stops=row["stops"],
                )
                for row in route_trips_data.iter_rows(named=True)
            ]

            # Deduplicate stop sets
            trip_sequences = deduplicate_trips(trip_sequences)

            # Process each unique stop pattern
            for trip_sequence in trip_sequences:
                # Get the stop locations (with lat and long)
                stop_locations = self._get_stop_locations(trip_sequence.stops, stops)
                stop_objects = []
                if not self.exclude_stops:
                    stop_objects = self._get_stop_objects(
                        stop_locations, self.add_missing_stops, self.search_radius
                    )

                # Get the OSM ways that make up the route
                osm_way_ids = []
                if not self.exclude_routes:
                    osm_way_ids = self._get_route_ways(trip_sequence.shape_id, shapes)

                # Calculate direction
                direction = ""
                if self.route_direction:
                    first_stop = stop_locations.head(1).select(["lat", "lon"]).row(0)
                    last_stop = stop_locations.tail(1).select(["lat", "lon"]).row(0)
                    direction = calculate_direction(first_stop, last_stop)

                route_tags = {
                    "type": "route",
                    "public_transport:version": "2",
                    "route": self._get_osm_route_type(route_info[5]),
                    "ref": route_info[2],
                    "name": f"Route {route_info[2]} {format_name(route_info[3])} {direction}".strip(),
                }
                if route_info[7] and len(route_info[7].strip("#")) in (3, 6):
                    route_tags["colour"] = "#" + route_info[7].strip("#")

                if self.relation_tags:
                    route_tags.update(self.relation_tags)

                # Create OSM relation
                relation = OSMRelation(
                    id=-1 * random.randint(1, string_to_unique_int(route_ref)),
                    tags=route_tags,
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

    def _get_stop_objects(
        self, stops: pl.DataFrame, add_missing_stops: bool, max_distance: float = 5
    ) -> list[OSMElement]:
        """
        Get OSM elements for stops by querying nearby bus stops from OpenStreetMap.

        Args:
            stops: DataFrame containing stop information with lat, lon, name, and stop_id columns
            add_missing_stops: Whether to add stops missing from the database to the output

        Returns:
            List of OSMElement objects representing nearby bus stops, ordered to match input stops
        """
        if stops.is_empty():
            return []

        osm_elements = []
        # overpass_url = "https://overpass-api.de/api/interpreter"
        overpass_url = "https://maps.mail.ru/osm/tools/overpass/api/interpreter"

        # Build a single query with all coordinates
        around_clauses = [
            f"(around:{max_distance},{stop_row['lat']},{stop_row['lon']})"
            for stop_row in stops.iter_rows(named=True)
        ]

        query_body = "\n".join(
            f"""
        node["highway"="bus_stop"]{around};
        node["public_transport"="platform"]{around};
        """
            for around in around_clauses
        )

        query = f"""
        [out:json][timeout:60];
        (
        {query_body}
        );
        out skel;
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
                    all_osm_nodes: list[OSMNode] = []
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
                                distance < min_distance and distance <= max_distance
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
                                f"No OSM stop found within {max_distance}m of GTFS stop at {stop_lat}, {stop_lon}"
                            )
                            if add_missing_stops:
                                # Add the stop to the output
                                new_stop = OSMNode(
                                    id=-1 * stop_row["stop_id"],
                                    lat=stop_lat,
                                    lon=stop_lon,
                                    tags={
                                        "name": stop_row["name"],
                                        "public_transport": "platform",
                                        "highway": "bus_stop",
                                    },
                                )
                                if not self.is_stop_duplicate(new_stop):
                                    logger.debug(f"Adding new stop {new_stop.id}")
                                    self.new_stops.append(new_stop)
                                else:
                                    logger.debug(
                                        f"Skipping duplicate stop {new_stop.id}"
                                    )
                                osm_elements.append(new_stop)

                    # Success - break out of retry loop
                    break

                elif response.status_code in [429, 504]:
                    retry_count += 1
                    error_base = (
                        f"Request error by Overpass API (HTTP {response.status_code})."
                    )
                    if retry_count <= max_retries:
                        logger.warning(
                            " ".join(
                                [
                                    error_base,
                                    "Retrying in 2 seconds...",
                                    f"(attempt {retry_count + 1}/{max_retries + 1})",
                                ]
                            )
                        )
                        time.sleep(2)
                    else:
                        logger.error(
                            " ".join(
                                [error_base, f"Max retries ({max_retries}) exceeded."]
                            )
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

        # After processing all stops, batch check for duplicates
        if add_missing_stops:
            existing_stop_ids = {stop.id for stop in self.new_stops}
            new_stops_to_add = []

            for element in osm_elements:
                if element.id < 0 and element.id not in existing_stop_ids:
                    new_stops_to_add.append(element)
                    existing_stop_ids.add(
                        element.id
                    )  # Prevent duplicates within this batch

            self.new_stops.extend(new_stops_to_add)
            logger.debug(f"Added {len(new_stops_to_add)} new stops")

        logger.info(
            f"Found {len([i for i in osm_elements if i.id > 0])} OSM stop elements, created {len([i for i in osm_elements if i.id < 0])} (ordered)"
        )
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
        self, stop_ids: list[int], stops: pl.DataFrame
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
        shape_id: str | int,
        shapes: pl.DataFrame,
        costing: str = "bus",
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ) -> list[int]:
        """
        Get OSM way IDs for a route between stops using Valhalla API.

        Args:
            shape_id: GTFS shape ID
            shapes: Complete list of GTFS shapes data
            costing: Valhalla costing model to use (bus, auto, pedestrian, etc.)
            max_retries: Maximum number of retry attempts if request fails
            retry_delay: Delay in seconds between retry attempts

        Returns:
            List of unique OSM way IDs that make up the route
        """
        logger.info(f"Getting OSM ways for route {shape_id}")

        filtered_shapes = shapes.filter(pl.col("shape_id") == shape_id).sort(
            "shape_pt_sequence"
        )

        route_ways = []

        # Get the input data for the request
        valhalla_url = "https://valhalla1.openstreetmap.de/trace_attributes"

        request_json = {
            "shape": filtered_shapes.select(
                [
                    pl.struct(
                        [
                            pl.col("shape_pt_lat").alias("lat"),
                            pl.col("shape_pt_lon").alias("lon"),
                        ]
                    )
                ]
            )
            .to_series()
            .to_list(),
            "costing": costing,
            "costing_options": {
                "maneuver_penalty": 10,
                "ignore_access": True,
                "ignore_restrictions": True,
            },
            "format": "default",
            "shape_match": "map_snap",
            "filters": {"attributes": ["edge.way_id"]},
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
                            # Prevent repeated way IDs
                            if not route_ways or way_id != route_ways[-1]:
                                route_ways.append(way_id)

                    logger.info(
                        f"Found {len(route_ways)} total ways for route {shape_id} variant"
                    )
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

        logger.info(f"Found {len(route_ways)} total ways for route {shape_id} variant")

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

    def is_stop_duplicate(self, new_stop):
        """Check if a stop is already in self.new_stops."""
        for existing_stop in self.new_stops:
            # Check if ID matches
            if existing_stop.id == new_stop.id:
                return True

        return False

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

                # Write new stops
                for stop in self.new_stops:
                    f.write(stop.to_xml() + "\n")

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
            raise OSError(f"Failed to write OSM file: {str(e)}") from e


def convert_gtfs_to_osm(gtfs_path: str, osm_path: str, **options: dict) -> bool:
    """
    Convert a GTFS feed to OSM relations.

    Args:
        gtfs_path: Path to the GTFS feed zip file
        osm_path: Path to write the OSM XML file
        **options: Additional options for the conversion
            - exclude_stops: Whether to include stops (default: False)
            - exclude_routes: Whether to include routes (default: False)
            - add_missing_stops: Whether to add stops missing from the database to the output (default: False)
            - stop_search_radius: Radius in meters to search for stops (default: 10)
            - add_route_direction: Whether to add route direction to the output (default: False)
            - route_ref_pattern: Regex pattern to filter routes by (default: None)
            - relation_tags: Dict of tags to add to the route and route_master relations (default: None)

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
            exclude_stops=options.get("exclude_stops", False),
            exclude_routes=options.get("exclude_routes", False),
            add_missing_stops=options.get("add_missing_stops", False),
            # route_types=options.get("route_types"),
            # agency_id=options.get("agency_id"),
            search_radius=options.get("stop_search_radius", 10.0),
            route_direction=options.get("route_direction", False),
            route_ref_pattern=options.get("route_ref_pattern"),
            relation_tags=options.get("relation_tags"),
        )

        logger.debug(f"OSMRelationBuilder options: {builder}")

        builder.build_relations(loader.tables)
        builder.build_route_masters(loader.tables)

        # Write to file
        builder.write_to_file(osm_path)

        return True

    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}")
        raise
