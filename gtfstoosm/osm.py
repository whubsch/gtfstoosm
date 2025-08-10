from typing import Literal
from pydantic import BaseModel, Field
import datetime
import logging


logger = logging.getLogger(__name__)


class OSMElement(BaseModel):
    id: int
    tags: dict[str, str] = Field(default_factory=dict)

    def add_tag(self, key: str, value: str) -> None:
        """Add a tag to the element."""
        if key in self.tags:
            raise ValueError(f"Key {key} already exists with value: {value}")
        if value is not None and value != "":
            self.tags[key] = value

    def modify_tag(self, key: str, value: str) -> None:
        """Modify a tag in the element."""
        if key not in self.tags:
            raise ValueError(f"Key {key} does not yet exist")
        if value is not None and value != "":
            self.tags[key] = value

    def tags_to_xml(self) -> str:
        """Create an XML tag element."""
        return "\n".join(
            [f'<tag k="{key}" v="{value}"></tag>' for key, value in self.tags.items()]
        )


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
        osm_text = f'<node id="{self.id}" lat="{self.lat}" lon="{self.lon}">'
        osm_text += self.tags_to_xml()
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

    def to_xml(self) -> str:
        osm_text = f'<way id="{self.id}" visible="{self.visible}">'
        osm_text += "\n".join([f"<nd ref='{node_id}'></nd>" for node_id in self.nodes])
        osm_text += self.tags_to_xml()
        return osm_text + "</way>"


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
        self, osm_type: Literal["node", "way", "relation"], ref: int, role: str = ""
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
        osm_text += self.tags_to_xml()

        return osm_text + "</relation>"
