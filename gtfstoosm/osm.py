from typing import Literal, Any
from pydantic import BaseModel, Field
import xml.dom.minidom
import datetime
import logging

logger = logging.getLogger(__name__)


class OSMElement(BaseModel):
    id: int
    tags: dict[str, str] = Field(default_factory=dict)

    def add_tag(self, key: str, value: str) -> None:
        """Add a tag to the element."""
        if value is not None and value != "":
            self.tags[key] = value


class OSMNode(OSMElement):
    lat: float
    lon: float
    visible: bool = True
    version: int = 1
    changeset: int = 1
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    user: str = "gtfstoosm"
    uid: int = 1

    def to_xml(self) -> str:
        osm_text = f'<node id="{self.id}" lat="{self.lat}" lon="{self.lon}"'
        osm_text += "\n".join(
            [f"<tag k='{key}' v='{value}'></tag>" for key, value in self.tags.items()]
        )
        osm_text += "</node>"
        return osm_text


class OSMWay(OSMElement):
    nodes: list[int] = Field(default_factory=list)
    visible: bool = True
    version: int = 1
    changeset: int = 1
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    user: str = "gtfstoosm"
    uid: int = 1

    def add_node(self, node_id: int) -> None:
        """Add a node to the way."""
        self.nodes.append(node_id)


class RelationMember(BaseModel):
    type: Literal["node", "way", "relation"]
    ref: int
    role: str

    def to_xml(self) -> str:
        """Create an XML member element."""
        return (
            f'<member type="{self.type}" ref="{self.ref}" role="{self.role}"></member>'
        )


class OSMRelation(OSMElement):
    members: list[RelationMember] = Field(default_factory=list)
    visible: bool = True
    version: int = 1
    changeset: int = 1
    timestamp: datetime.datetime = Field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc)
    )
    user: str = "gtfstoosm"
    uid: int = 1

    def add_member(
        self, osm_type: Literal["node", "way", "relation"], ref: int, role: str
    ) -> None:
        """Add a member to the relation."""
        # Validate type
        if osm_type not in ["node", "way", "relation"]:
            raise ValueError(f"Invalid member type: {osm_type}")

        self.members.append(RelationMember(type=osm_type, ref=ref, role=role))

    def to_xml(self) -> str:
        """Create an XML relation element."""
        osm_text = f'<relation id="{self.id}" visible="{self.visible}">'

        osm_text += "\n".join([member.to_xml() for member in self.members])
        osm_text += "\n".join(
            [f"<tag k='{key}' v='{value}'></tag>" for key, value in self.tags.items()]
        )

        return osm_text + "</relation>"


class OSMDocument(BaseModel):
    nodes: dict[int, OSMNode] = Field(default_factory=dict)
    ways: dict[int, OSMWay] = Field(default_factory=dict)
    relations: dict[int, OSMRelation] = Field(default_factory=dict)
    version: str = "0.6"
    generator: str = "gtfstoosm"
    current_id: int = -1

    def add_node(self, node: OSMNode) -> OSMNode:
        """Add a node to the document."""
        self.nodes[node.id] = node
        return node

    def add_way(self, way: OSMWay) -> OSMWay:
        """Add a way to the document."""
        self.ways[way.id] = way
        return way

    def add_relation(self, relation: OSMRelation) -> OSMRelation:
        """Add a relation to the document."""
        self.relations[relation.id] = relation
        return relation

    def generate_id(self) -> int:
        """Generate a new ID."""
        self.current_id -= 1
        return self.current_id

    def to_xml(self) -> str:
        """Create an XML document."""
        doc = xml.dom.minidom.getDOMImplementation().createDocument(None, "osm", None)
        root = doc.documentElement
        root.setAttribute("version", self.version)
        root.setAttribute("generator", self.generator)

        # Add nodes
        for node in self.nodes.values():
            root.appendChild(node.to_xml())

        # Add ways
        for way in self.ways.values():
            root.appendChild(way.to_xml(doc))

        # Add relations
        for relation in self.relations.values():
            root.appendChild(relation.to_xml(doc))

        return doc.toprettyxml(indent="  ")

    def write_to_file(self, filename: str) -> None:
        """Write the XML document to a file."""
        with open(filename, "w", encoding="utf-8") as file:
            file.write(self.to_xml())


class OSMBuilder:
    """Class for building OSM documents from GTFS data."""

    def __init__(self):
        """Initialize the OSM builder."""
        self.document = OSMDocument()
        self.gtfs_stop_id_to_osm_node_id = {}  # Mapping of GTFS stop IDs to OSM node IDs

    def create_stop_node(
        self, stop_id: str, lat: float, lon: float, tags: dict[str, str]
    ) -> int:
        """
        Create a node for a GTFS stop.

        Args:
            stop_id: GTFS stop ID
            lat: Latitude
            lon: Longitude
            tags: Dictionary of tags for the node

        Returns:
            OSM node ID
        """
        if stop_id in self.gtfs_stop_id_to_osm_node_id:
            return self.gtfs_stop_id_to_osm_node_id[stop_id]

        osm_id = self.document.generate_id()
        node = OSMNode(**{"id": osm_id, "lat": lat, "lon": lon, "tags": tags})
        self.document.add_node(node)

        # Store the mapping
        self.gtfs_stop_id_to_osm_node_id[stop_id] = osm_id

        return osm_id

    def create_route_relation(
        self, route_id: str, tags: dict[str, str], stop_ids: list[int] = None
    ) -> int:
        """
        Create a relation for a GTFS route.

        Args:
            route_id: GTFS route ID
            tags: Dictionary of tags for the relation
            stop_ids: Optional list of GTFS stop IDs to include as members

        Returns:
            OSM relation ID
        """
        osm_id = self.document.generate_id()
        relation = OSMRelation(**{"id": osm_id, "tags": tags})

        # Add stops as members if provided
        if stop_ids:
            for stop_id in stop_ids:
                if stop_id in self.gtfs_stop_id_to_osm_node_id:
                    osm_node_id = self.gtfs_stop_id_to_osm_node_id[stop_id]
                    relation.add_member("node", osm_node_id, "platform")

        self.document.add_relation(relation)
        return osm_id

    def create_route_master_relation(
        self, name: str, route_type: str, route_ref: str, route_relation_ids: list[int]
    ) -> int:
        """
        Create a route_master relation grouping multiple route variants.

        Args:
            name: Name of the route master
            route_type: Type of route (bus, train, etc.)
            route_ref: Route reference/number
            route_relation_ids: List of OSM relation IDs for route variants

        Returns:
            OSM relation ID
        """
        osm_id = self.document.generate_id()

        # Create tags for the route_master
        tags = {
            "type": "route_master",
            "route_master": route_type,
            "name": name,
            "ref": route_ref,
        }

        relation = OSMRelation(**{"id": osm_id, "tags": tags})

        # Add route relations as members
        for route_rel_id in route_relation_ids:
            relation.add_member("relation", route_rel_id, "")

        self.document.add_relation(relation)
        return osm_id

    def build_from_gtfs(
        self,
        stops: list[dict[str, Any]],
        routes: list[dict[str, Any]],
        route_stops: dict[str, list[dict[str, Any]]],
        agency_name: str | None = None,
    ) -> None:
        """
        Build an OSM document from GTFS data.

        Args:
            stops: List of GTFS stop dictionaries
            routes: List of GTFS route dictionaries
            route_stops: Dictionary mapping route IDs to lists of stop dictionaries
            agency_name: Optional agency name for the network tag
        """
        logger.info("Building OSM document from GTFS data")

        # Process stops
        for stop in stops:
            stop_id = stop.get("stop_id")
            lat = stop.get("stop_lat")
            lon = stop.get("stop_lon")
            stop_name = stop.get("stop_name", "")

            if stop_id and lat is not None and lon is not None:
                # Create basic tags
                tags = {
                    "name": stop_name,
                    "public_transport": "platform",
                    "gtfs:stop_id": str(stop_id),
                }

                # Add optional tags if present
                if stop.get("stop_code"):
                    tags["ref"] = stop["stop_code"]
                if stop.get("wheelchair_boarding") == 1:
                    tags["wheelchair"] = "yes"
                elif stop.get("wheelchair_boarding") == 2:
                    tags["wheelchair"] = "no"

                # Create the node
                self.create_stop_node(stop_id, lat, lon, tags)

        # Process routes
        route_relations_by_ref = {}  # Group route relations by route_ref for route_master

        for route in routes:
            route_id = route.get("route_id")
            route_short_name = route.get("route_short_name", "")
            route_long_name = route.get("route_long_name", "")
            route_type = route.get("route_type", 3)  # Default to bus (3)

            if not route_id:
                continue

            # Map GTFS route_type to OSM route type
            osm_route_type = self._map_route_type(route_type)

            # Create route tags
            tags = {
                "type": "route",
                "route": osm_route_type,
                "ref": route_short_name,
                "name": route_long_name or route_short_name,
                "gtfs:route_id": str(route_id),
            }

            if agency_name:
                tags["network"] = agency_name

            if route.get("route_color"):
                tags["colour"] = "#" + route["route_color"]

            # Get stops for this route
            stops_for_route = route_stops.get(route_id, [])
            stop_ids: list[int] = [
                int(stop["stop_id"]) for stop in stops_for_route if "stop_id" in stop
            ]

            # Create the relation
            relation_id = self.create_route_relation(route_id, tags, stop_ids)

            # Group by route_ref for route_master relations
            if route_short_name:
                if route_short_name not in route_relations_by_ref:
                    route_relations_by_ref[route_short_name] = {
                        "type": osm_route_type,
                        "name": route_long_name or route_short_name,
                        "relations": [],
                    }
                route_relations_by_ref[route_short_name]["relations"].append(
                    relation_id
                )

        # Create route_master relations for routes with the same ref
        for route_ref, info in route_relations_by_ref.items():
            if len(info["relations"]) > 1:
                self.create_route_master_relation(
                    info["name"], info["type"], route_ref, info["relations"]
                )

        logger.info(
            f"Built OSM document with {len(self.document.nodes)} nodes and "
            f"{len(self.document.relations)} relations"
        )

    def _map_route_type(self, gtfs_route_type: int | str) -> str:
        """
        Map GTFS route_type to OSM route tag value.

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
            6: "aerialway",
            7: "funicular",
            11: "trolleybus",
            12: "monorail",
        }

        return route_type_map.get(route_type, "bus")

    def get_document(self) -> OSMDocument:
        """
        Get the built OSM document.

        Returns:
            OSMDocument
        """
        return self.document

    def write_to_file(self, filename: str) -> None:
        """
        Write the OSM document to a file.

        Args:
            filename: Path to the output file
        """
        self.document.write_to_file(filename)


def create_osm_from_gtfs(
    stops: list[dict[str, Any]],
    routes: list[dict[str, Any]],
    route_stops: dict[str, list[dict[str, Any]]],
    output_file: str,
    agency_name: str | None = None,
) -> None:
    """
    Create an OSM file from GTFS data.

    Args:
        stops: List of GTFS stop dictionaries
        routes: List of GTFS route dictionaries
        route_stops: Dictionary mapping route IDs to lists of stop dictionaries
        output_file: Path to the output OSM file
        agency_name: Optional agency name for the network tag

    Raises:
        IOError: If writing to the file fails
    """
    builder = OSMBuilder()
    builder.build_from_gtfs(stops, routes, route_stops, agency_name)
    builder.write_to_file(output_file)
