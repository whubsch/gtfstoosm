"""Tests for utility functions."""

import pytest

from gtfstoosm.utils import (
    Trip,
    calculate_direction,
    create_bounding_box,
    deduplicate_trips,
    format_name,
    parse_tag_string,
    string_to_unique_int,
)


class TestStringToUniqueInt:
    """Tests for string_to_unique_int function."""

    def test_same_string_same_hash(self):
        """Test that the same string always produces the same hash."""
        result1 = string_to_unique_int("test_string")
        result2 = string_to_unique_int("test_string")
        assert result1 == result2

    def test_different_strings_different_hash(self):
        """Test that different strings produce different hashes."""
        result1 = string_to_unique_int("string1")
        result2 = string_to_unique_int("string2")
        assert result1 != result2

    def test_positive_integer(self):
        """Test that the result is always positive."""
        result = string_to_unique_int("test")
        assert result > 0

    def test_within_max_int(self):
        """Test that the result is within the specified maximum."""
        max_int = 1000
        result = string_to_unique_int("test", max_int=max_int)
        assert 0 < result < max_int

    def test_empty_string(self):
        """Test with an empty string."""
        result = string_to_unique_int("")
        assert isinstance(result, int)
        assert result > 0


class TestDeduplicateTrips:
    """Tests for deduplicate_trips function."""

    def test_no_duplicates(self):
        """Test with trips that have unique stops."""
        trips = [
            Trip(trip_id=1, route_id="R1", shape_id="S1", stops=[1, 2, 3]),
            Trip(trip_id=2, route_id="R1", shape_id="S2", stops=[4, 5, 6]),
        ]
        result = deduplicate_trips(trips)
        assert len(result) == 2

    def test_with_duplicates(self):
        """Test with trips that have duplicate stops."""
        trips = [
            Trip(trip_id=1, route_id="R1", shape_id="S1", stops=[1, 2, 3]),
            Trip(trip_id=2, route_id="R1", shape_id="S2", stops=[1, 2, 3]),
            Trip(trip_id=3, route_id="R1", shape_id="S3", stops=[4, 5, 6]),
        ]
        result = deduplicate_trips(trips)
        assert len(result) == 2
        # Should keep first occurrence
        assert result[0].trip_id == 1
        assert result[1].trip_id == 3

    def test_keeps_first_occurrence(self):
        """Test that first occurrence is kept when duplicates exist."""
        trips = [
            Trip(trip_id=1, route_id="R1", shape_id="S1", stops=[1, 2, 3]),
            Trip(trip_id=2, route_id="R1", shape_id="S2", stops=[1, 2, 3]),
        ]
        result = deduplicate_trips(trips)
        assert len(result) == 1
        assert result[0].trip_id == 1

    def test_empty_list(self):
        """Test with an empty list."""
        result = deduplicate_trips([])
        assert result == []

    def test_order_matters(self):
        """Test that stop order matters for deduplication."""
        trips = [
            Trip(trip_id=1, route_id="R1", shape_id="S1", stops=[1, 2, 3]),
            Trip(trip_id=2, route_id="R1", shape_id="S2", stops=[3, 2, 1]),
        ]
        result = deduplicate_trips(trips)
        assert len(result) == 2


class TestCalculateDirection:
    """Tests for calculate_direction function."""

    def test_northbound(self):
        """Test northbound direction calculation."""
        start = (40.0, -74.0)
        end = (41.0, -74.0)
        assert calculate_direction(start, end) == "Northbound"

    def test_southbound(self):
        """Test southbound direction calculation (note: typo in source code)."""
        start = (41.0, -74.0)
        end = (40.0, -74.0)
        # Note: Source has typo "Soutbound"
        assert calculate_direction(start, end) == "Soutbound"

    def test_eastbound(self):
        """Test eastbound direction calculation."""
        start = (40.0, -75.0)
        end = (40.0, -74.0)
        assert calculate_direction(start, end) == "Eastbound"

    def test_westbound(self):
        """Test westbound direction calculation."""
        start = (40.0, -74.0)
        end = (40.0, -75.0)
        assert calculate_direction(start, end) == "Westbound"

    def test_string_coordinates(self):
        """Test with string coordinates (should be converted to float)."""
        start = ("40.0", "-74.0")
        end = ("41.0", "-74.0")
        assert calculate_direction(start, end) == "Northbound"


class TestFormatName:
    """Tests for format_name function."""

    def test_basic_formatting(self):
        """Test basic name formatting."""
        assert "Street" in format_name("main street")

    def test_strip_whitespace(self):
        """Test stripping leading/trailing whitespace."""
        result = format_name("  test name  ")
        assert not result.startswith(" ")
        assert not result.endswith(" ")

    def test_strip_punctuation(self):
        """Test stripping trailing punctuation."""
        result = format_name("test name,;")
        assert not result.endswith(",")
        assert not result.endswith(";")

    def test_double_spaces(self):
        """Test replacing double spaces with single space."""
        result = format_name("test  name")
        assert "  " not in result

    def test_underscores(self):
        """Test replacing underscores with spaces."""
        result = format_name("test_name")
        assert "_" not in result
        assert " " in result

    def test_xml_escaping(self):
        """Test XML character escaping."""
        result = format_name("test & name < > value")
        assert "&amp;" in result
        assert "&lt;" in result
        assert "&gt;" in result

    def test_separator_preservation(self):
        """Test that separators are preserved."""
        for separator in ["/", "-", "–", "—", "|", "\\", "~"]:
            result = format_name(f"north{separator}south")
            assert separator in result

    def test_empty_string(self):
        """Test with empty string."""
        result = format_name("")
        assert result == ""


class TestCreateBoundingBox:
    """Tests for create_bounding_box function."""

    def test_creates_four_coordinates(self):
        """Test that function returns four coordinates."""
        result = create_bounding_box(40.7128, -74.0060, 100)
        assert len(result) == 4

    def test_returns_strings(self):
        """Test that all values are strings."""
        result = create_bounding_box(40.7128, -74.0060, 100)
        assert all(isinstance(x, str) for x in result)

    def test_min_less_than_max(self):
        """Test that min values are less than max values."""
        result = create_bounding_box(40.7128, -74.0060, 100)
        min_lat, min_lon, max_lat, max_lon = [float(x) for x in result]
        assert min_lat < max_lat
        assert min_lon < max_lon

    def test_center_within_box(self):
        """Test that center point is within the bounding box."""
        lat, lon = 40.7128, -74.0060
        result = create_bounding_box(lat, lon, 100)
        min_lat, min_lon, max_lat, max_lon = [float(x) for x in result]
        assert min_lat < lat < max_lat
        assert min_lon < lon < max_lon

    def test_zero_distance(self):
        """Test with zero distance."""
        lat, lon = 40.7128, -74.0060
        result = create_bounding_box(lat, lon, 0)
        min_lat, min_lon, max_lat, max_lon = [float(x) for x in result]
        # Should be approximately equal (within floating point precision)
        assert abs(float(min_lat) - lat) < 1e-10
        assert abs(float(max_lat) - lat) < 1e-10

    def test_equator(self):
        """Test at the equator where longitude calculation is simplest."""
        result = create_bounding_box(0.0, 0.0, 1000)
        assert len(result) == 4

    def test_positive_distance_only(self):
        """Test that distance creates expansion, not contraction."""
        lat, lon = 40.7128, -74.0060
        result = create_bounding_box(lat, lon, 100)
        min_lat, min_lon, max_lat, max_lon = [float(x) for x in result]

        # Box should be larger than a point
        assert max_lat - min_lat > 0
        assert max_lon - min_lon > 0


class TestParseTagString:
    """Tests for parse_tag_string function."""

    def test_single_tag(self):
        """Test parsing a single key-value pair."""
        result = parse_tag_string("key=value")
        assert result == {"key": "value"}

    def test_multiple_tags(self):
        """Test parsing multiple key-value pairs."""
        result = parse_tag_string("operator=Transit;network=City Bus")
        assert result == {"operator": "Transit", "network": "City Bus"}

    def test_with_whitespace(self):
        """Test that whitespace is trimmed."""
        result = parse_tag_string(" key = value ; key2 = value2 ")
        assert result == {"key": "value", "key2": "value2"}

    def test_value_with_equals(self):
        """Test value containing equals sign."""
        result = parse_tag_string("url=https://example.com;name=Test")
        assert result == {"url": "https://example.com", "name": "Test"}

    def test_colon_in_key(self):
        """Test key containing colon (common in OSM tags)."""
        result = parse_tag_string("network:wikidata=Q123;operator=Test")
        assert result == {"network:wikidata": "Q123", "operator": "Test"}

    def test_empty_string(self):
        """Test with empty string."""
        result = parse_tag_string("")
        assert result == {}

    def test_malformed_pair(self):
        """Test that malformed pairs are skipped."""
        result = parse_tag_string("malformed;key=value")
        assert result == {"key": "value"}

    def test_only_separator(self):
        """Test with only separators."""
        result = parse_tag_string(";;;")
        assert result == {}

    def test_complex_example(self):
        """Test complex real-world example."""
        result = parse_tag_string(
            "operator=TransitCenter;network=Whoville Bus;network:wikidata=Q123"
        )
        assert result == {
            "operator": "TransitCenter",
            "network": "Whoville Bus",
            "network:wikidata": "Q123",
        }

    def test_preserves_spaces_in_values(self):
        """Test that spaces within values are preserved."""
        result = parse_tag_string("name=New York City Bus")
        assert result == {"name": "New York City Bus"}
