import datetime

import pytest

from gtfstoosm.osm import (
    OSMElement,
    OSMNode,
    OSMRelation,
    OSMWay,
    RelationMember,
)


class TestOSMElement:
    """Tests for the base OSMElement class."""

    def test_osm_element_creation(self):
        """Test basic OSMElement creation."""
        element = OSMElement(id=1)
        assert element.id == 1
        assert element.tags == {}

    def test_osm_element_with_tags(self):
        """Test OSMElement creation with tags."""
        element = OSMElement(id=1, tags={"name": "Test", "type": "example"})
        assert element.tags == {"name": "Test", "type": "example"}

    def test_add_tag_success(self):
        """Test adding a tag to an element."""
        element = OSMElement(id=1)
        element.add_tag("name", "Test Stop")
        assert element.tags["name"] == "Test Stop"

    def test_add_tag_duplicate_key_raises_error(self):
        """Test that adding a duplicate key raises ValueError."""
        element = OSMElement(id=1)
        element.add_tag("name", "Test Stop")
        with pytest.raises(ValueError, match="Key name already exists"):
            element.add_tag("name", "Another Name")

    def test_add_tag_empty_value_not_added(self):
        """Test that empty string values are not added."""
        element = OSMElement(id=1)
        element.add_tag("name", "")
        assert "name" not in element.tags

    def test_add_tag_none_value_not_added(self):
        """Test that None values are not added."""
        element = OSMElement(id=1)
        element.add_tag("name", None)
        assert "name" not in element.tags

    def test_modify_tag_success(self):
        """Test modifying an existing tag."""
        element = OSMElement(id=1, tags={"name": "Old Name"})
        element.modify_tag("name", "New Name")
        assert element.tags["name"] == "New Name"

    def test_modify_tag_nonexistent_key_raises_error(self):
        """Test that modifying a non-existent key raises ValueError."""
        element = OSMElement(id=1)
        with pytest.raises(ValueError, match="Key name does not yet exist"):
            element.modify_tag("name", "Test")

    def test_modify_tag_empty_value_not_modified(self):
        """Test that empty string values don't modify the tag."""
        element = OSMElement(id=1, tags={"name": "Original"})
        element.modify_tag("name", "")
        assert element.tags["name"] == "Original"

    def test_modify_tag_none_value_not_modified(self):
        """Test that None values don't modify the tag."""
        element = OSMElement(id=1, tags={"name": "Original"})
        element.modify_tag("name", None)
        assert element.tags["name"] == "Original"

    def test_tags_to_xml_empty(self):
        """Test XML generation with no tags."""
        element = OSMElement(id=1)
        assert element.tags_to_xml() == ""

    def test_tags_to_xml_single_tag(self):
        """Test XML generation with a single tag."""
        element = OSMElement(id=1, tags={"name": "Test"})
        xml = element.tags_to_xml()
        assert xml == '<tag k="name" v="Test"></tag>'

    def test_tags_to_xml_multiple_tags(self):
        """Test XML generation with multiple tags."""
        element = OSMElement(id=1, tags={"name": "Test", "highway": "bus_stop"})
        xml = element.tags_to_xml()
        assert '<tag k="name" v="Test"></tag>' in xml
        assert '<tag k="highway" v="bus_stop"></tag>' in xml
        assert xml.count("\n") == 1  # One newline separator


class TestOSMNode:
    """Tests for the OSMNode class."""

    def test_osm_node_creation(self):
        """Test basic OSMNode creation."""
        node = OSMNode(id=1, lat=40.7128, lon=-74.0060)
        assert node.id == 1
        assert node.lat == 40.7128
        assert node.lon == -74.0060
        assert node.visible is True
        assert node.version == 1
        assert node.changeset == 1
        assert node.user == "gtfstoosm"
        assert node.uid == 1

    def test_osm_node_with_tags(self):
        """Test OSMNode creation with tags."""
        node = OSMNode(
            id=1,
            lat=40.7128,
            lon=-74.0060,
            tags={"name": "Test Stop", "public_transport": "stop_position"},
        )
        assert node.tags["name"] == "Test Stop"
        assert node.tags["public_transport"] == "stop_position"

    def test_osm_node_timestamp_default(self):
        """Test that timestamp is automatically set."""
        node = OSMNode(id=1, lat=40.7128, lon=-74.0060)
        assert isinstance(node.timestamp, datetime.datetime)
        assert node.timestamp.tzinfo == datetime.timezone.utc

    def test_osm_node_custom_timestamp(self):
        """Test OSMNode with custom timestamp."""
        custom_time = datetime.datetime(
            2023, 1, 1, 12, 0, 0, tzinfo=datetime.timezone.utc
        )
        node = OSMNode(id=1, lat=40.7128, lon=-74.0060, timestamp=custom_time)
        assert node.timestamp == custom_time

    def test_osm_node_to_xml_minimal(self):
        """Test XML generation for node without tags."""
        node = OSMNode(id=123, lat=40.7128, lon=-74.0060)
        xml = node.to_xml()
        assert xml.startswith('<node id="123" lat="40.7128" lon="-74.006">')
        assert xml.endswith("</node>")

    def test_osm_node_to_xml_with_tags(self):
        """Test XML generation for node with tags."""
        node = OSMNode(
            id=123,
            lat=40.7128,
            lon=-74.0060,
            tags={"name": "Test Stop", "highway": "bus_stop"},
        )
        xml = node.to_xml()
        assert '<node id="123"' in xml
        assert 'lat="40.7128"' in xml
        assert 'lon="-74.006"' in xml
        assert '<tag k="name" v="Test Stop"></tag>' in xml
        assert '<tag k="highway" v="bus_stop"></tag>' in xml
        assert xml.endswith("</node>")


class TestOSMWay:
    """Tests for the OSMWay class."""

    def test_osm_way_creation(self):
        """Test basic OSMWay creation."""
        way = OSMWay(id=1)
        assert way.id == 1
        assert way.nodes == []
        assert way.visible is True
        assert way.version == 1
        assert way.user == "gtfstoosm"

    def test_osm_way_with_nodes(self):
        """Test OSMWay creation with nodes."""
        way = OSMWay(id=1, nodes=[100, 101, 102])
        assert way.nodes == [100, 101, 102]

    def test_osm_way_add_node(self):
        """Test adding a node to a way."""
        way = OSMWay(id=1)
        way.add_node(100)
        way.add_node(101)
        assert way.nodes == [100, 101]

    def test_osm_way_add_node_to_existing(self):
        """Test adding nodes to a way with existing nodes."""
        way = OSMWay(id=1, nodes=[100, 101])
        way.add_node(102)
        assert way.nodes == [100, 101, 102]

    def test_osm_way_to_xml_minimal(self):
        """Test XML generation for way without nodes or tags."""
        way = OSMWay(id=456)
        xml = way.to_xml()
        assert xml.startswith('<way id="456" visible="True">')
        assert xml.endswith("</way>")

    def test_osm_way_to_xml_with_nodes(self):
        """Test XML generation for way with nodes."""
        way = OSMWay(id=456, nodes=[100, 101, 102])
        xml = way.to_xml()
        assert '<way id="456"' in xml
        assert "<nd ref='100'></nd>" in xml
        assert "<nd ref='101'></nd>" in xml
        assert "<nd ref='102'></nd>" in xml
        assert xml.endswith("</way>")

    def test_osm_way_to_xml_with_tags(self):
        """Test XML generation for way with tags."""
        way = OSMWay(id=456, tags={"highway": "primary", "name": "Main Street"})
        xml = way.to_xml()
        assert '<tag k="highway" v="primary"></tag>' in xml
        assert '<tag k="name" v="Main Street"></tag>' in xml

    def test_osm_way_to_xml_complete(self):
        """Test XML generation for way with both nodes and tags."""
        way = OSMWay(
            id=456,
            nodes=[100, 101],
            tags={"highway": "primary"},
        )
        xml = way.to_xml()
        assert '<way id="456"' in xml
        assert "<nd ref='100'></nd>" in xml
        assert "<nd ref='101'></nd>" in xml
        assert '<tag k="highway" v="primary"></tag>' in xml
        assert xml.endswith("</way>")


class TestRelationMember:
    """Tests for the RelationMember class."""

    def test_relation_member_node(self):
        """Test creating a relation member of type node."""
        member = RelationMember(type="node", ref=123, role="stop")
        assert member.type == "node"
        assert member.ref == 123
        assert member.role == "stop"

    def test_relation_member_way(self):
        """Test creating a relation member of type way."""
        member = RelationMember(type="way", ref=456, role="platform")
        assert member.type == "way"
        assert member.ref == 456
        assert member.role == "platform"

    def test_relation_member_relation(self):
        """Test creating a relation member of type relation."""
        member = RelationMember(type="relation", ref=789, role="")
        assert member.type == "relation"
        assert member.ref == 789
        assert member.role == ""

    def test_relation_member_to_xml(self):
        """Test XML generation for relation member."""
        member = RelationMember(type="node", ref=123, role="stop")
        xml = member.to_xml()
        assert xml == '<member type="node" ref="123" role="stop"></member>'

    def test_relation_member_to_xml_empty_role(self):
        """Test XML generation for relation member with empty role."""
        member = RelationMember(type="way", ref=456, role="")
        xml = member.to_xml()
        assert xml == '<member type="way" ref="456" role=""></member>'


class TestOSMRelation:
    """Tests for the OSMRelation class."""

    def test_osm_relation_creation(self):
        """Test basic OSMRelation creation."""
        relation = OSMRelation(id=1)
        assert relation.id == 1
        assert relation.members == []
        assert relation.visible is True
        assert relation.version == 1

    def test_osm_relation_with_members(self):
        """Test OSMRelation creation with members."""
        members = [
            RelationMember(type="node", ref=100, role="stop"),
            RelationMember(type="way", ref=200, role="platform"),
        ]
        relation = OSMRelation(id=1, members=members)
        assert len(relation.members) == 2
        assert relation.members[0].type == "node"
        assert relation.members[1].type == "way"

    def test_osm_relation_add_member_node(self):
        """Test adding a node member to a relation."""
        relation = OSMRelation(id=1)
        relation.add_member("node", 100, "stop")
        assert len(relation.members) == 1
        assert relation.members[0].type == "node"
        assert relation.members[0].ref == 100
        assert relation.members[0].role == "stop"

    def test_osm_relation_add_member_way(self):
        """Test adding a way member to a relation."""
        relation = OSMRelation(id=1)
        relation.add_member("way", 200, "platform")
        assert len(relation.members) == 1
        assert relation.members[0].type == "way"

    def test_osm_relation_add_member_relation(self):
        """Test adding a relation member to a relation."""
        relation = OSMRelation(id=1)
        relation.add_member("relation", 300, "subrelation")
        assert len(relation.members) == 1
        assert relation.members[0].type == "relation"

    def test_osm_relation_add_member_empty_role(self):
        """Test adding a member with empty role."""
        relation = OSMRelation(id=1)
        relation.add_member("node", 100)
        assert relation.members[0].role == ""

    def test_osm_relation_add_member_invalid_type(self):
        """Test that invalid member type raises ValueError."""
        relation = OSMRelation(id=1)
        with pytest.raises(ValueError, match="Invalid member type"):
            relation.add_member("invalid_type", 100, "stop")

    def test_osm_relation_add_multiple_members(self):
        """Test adding multiple members to a relation."""
        relation = OSMRelation(id=1)
        relation.add_member("node", 100, "stop")
        relation.add_member("way", 200, "platform")
        relation.add_member("node", 101, "stop")
        assert len(relation.members) == 3

    def test_osm_relation_to_xml_minimal(self):
        """Test XML generation for relation without members or tags."""
        relation = OSMRelation(id=789)
        xml = relation.to_xml()
        assert xml.startswith('<relation id="789" visible="True">')
        assert xml.endswith("</relation>")

    def test_osm_relation_to_xml_with_members(self):
        """Test XML generation for relation with members."""
        relation = OSMRelation(id=789)
        relation.add_member("node", 100, "stop")
        relation.add_member("way", 200, "platform")
        xml = relation.to_xml()
        assert '<relation id="789"' in xml
        assert '<member type="node" ref="100" role="stop"></member>' in xml
        assert '<member type="way" ref="200" role="platform"></member>' in xml
        assert xml.endswith("</relation>")

    def test_osm_relation_to_xml_with_tags(self):
        """Test XML generation for relation with tags."""
        relation = OSMRelation(
            id=789,
            tags={"type": "route", "route": "bus", "name": "Route 1"},
        )
        xml = relation.to_xml()
        assert '<tag k="type" v="route"></tag>' in xml
        assert '<tag k="route" v="bus"></tag>' in xml
        assert '<tag k="name" v="Route 1"></tag>' in xml

    def test_osm_relation_to_xml_complete(self):
        """Test XML generation for relation with members and tags."""
        relation = OSMRelation(
            id=789,
            tags={"type": "route", "route": "bus"},
        )
        relation.add_member("node", 100, "stop")
        relation.add_member("way", 200, "platform")
        xml = relation.to_xml()
        assert '<relation id="789"' in xml
        assert '<member type="node" ref="100" role="stop"></member>' in xml
        assert '<member type="way" ref="200" role="platform"></member>' in xml
        assert '<tag k="type" v="route"></tag>' in xml
        assert '<tag k="route" v="bus"></tag>' in xml
        assert xml.endswith("</relation>")


class TestIntegration:
    """Integration tests for OSM elements."""

    def test_complete_bus_route_scenario(self):
        """Test creating a complete bus route with stops and platforms."""

        # Create a way for the route
        route_way = OSMWay(id=100, nodes=[1, 2])
        route_way.add_tag("highway", "primary")
        route_way.add_tag("name", "Main Street")

        # Create a relation for the bus route
        bus_route = OSMRelation(id=1000)
        bus_route.add_tag("type", "route")
        bus_route.add_tag("route", "bus")
        bus_route.add_tag("name", "Bus Route 1")
        bus_route.add_member("node", 1, "stop")
        bus_route.add_member("way", 100, "")
        bus_route.add_member("node", 2, "stop")

        # Verify the structure
        assert len(bus_route.members) == 3
        assert bus_route.tags["type"] == "route"

        # Test XML generation
        relation_xml = bus_route.to_xml()
        assert '<relation id="1000"' in relation_xml
        assert '<member type="node" ref="1" role="stop"></member>' in relation_xml
        assert '<member type="way" ref="100" role=""></member>' in relation_xml
        assert '<member type="node" ref="2" role="stop"></member>' in relation_xml
