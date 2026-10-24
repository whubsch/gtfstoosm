"""Tests for feeds that only provide the required GTFS columns.

Many published feeds omit optional routes.txt columns (route_desc, route_url,
route_color...), leave route_long_name empty, or use text IDs with leading
zeros. These tests run offline: the Overpass and Valhalla lookups are stubbed.
"""

import zipfile

import pytest

from gtfstoosm.convert import OSMRelationBuilder
from gtfstoosm.gtfs import GTFSFeed
from gtfstoosm.utils import format_colour, format_name


@pytest.fixture
def offline_builder(monkeypatch):
    """Return a builder factory whose network lookups return nothing."""
    monkeypatch.setattr(
        OSMRelationBuilder, "_get_stop_objects", lambda self, *a, **k: []
    )
    monkeypatch.setattr(OSMRelationBuilder, "_get_route_ways", lambda self, *a, **k: [])
    return OSMRelationBuilder


def make_feed(tmp_path, routes_csv):
    """Write a small feed with the given routes.txt and return it loaded."""
    zip_path = tmp_path / "sparse_gtfs.zip"
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
        "A,Agence,https://example.org,Europe/Paris\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
        "0101,Gare,44.54607,6.27145\n"
        "0102,Ecoles,44.5442,6.27793\n",
        "routes.txt": routes_csv,
        "trips.txt": "route_id,service_id,trip_id,shape_id\nLIGNE_01,S1,T1,SH1\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T1,08:00:00,08:00:00,0101,1\n"
        "T1,08:05:00,08:05:00,0102,2\n",
        "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
        "SH1,44.54607,6.27145,1\n"
        "SH1,44.5442,6.27793,2\n",
    }
    with zipfile.ZipFile(zip_path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)

    feed = GTFSFeed(feed_dir=str(zip_path))
    feed.load()
    return feed.tables


class TestSparseRoutes:
    """Conversion of routes.txt files without optional columns."""

    def test_only_required_columns_and_short_name(self, tmp_path, offline_builder):
        tables = make_feed(
            tmp_path, "route_id,route_short_name,route_type\nLIGNE_01,1,3\n"
        )
        builder = offline_builder()
        builder.build_relations(tables)

        [route] = builder.relations
        assert route.tags["ref"] == "1"
        assert route.tags["name"] == "Route 1"
        assert "colour" not in route.tags

    def test_empty_long_name_and_no_desc_url(self, tmp_path, offline_builder):
        tables = make_feed(
            tmp_path,
            "route_id,agency_id,route_short_name,route_long_name,route_type,"
            "route_color,route_text_color\n"
            "LIGNE_01,A,C1,,3,fda03f,\n",
        )
        builder = offline_builder()
        builder.build_relations(tables)

        [route] = builder.relations
        assert route.tags["name"] == "Route C1"
        assert route.tags["colour"] == "#fda03f"

    def test_long_name_only(self, tmp_path, offline_builder):
        tables = make_feed(
            tmp_path,
            "route_id,route_long_name,route_type\nLIGNE_01,Gare Ecoles,3\n",
        )
        builder = offline_builder()
        builder.build_relations(tables)
        builder.build_route_masters(tables)

        assert len(builder.relations) == 1
        assert builder.relations[0].tags["name"] == "Route Gare Ecoles"

    def test_black_colour_is_kept(self, tmp_path, offline_builder):
        tables = make_feed(
            tmp_path,
            "route_id,route_short_name,route_type,route_color\nLIGNE_01,1,3,000000\n",
        )
        builder = offline_builder()
        builder.build_relations(tables)

        assert builder.relations[0].tags["colour"] == "#000000"


class TestRouteMasters:
    """route_master creation when route_id differs from route_short_name."""

    def test_master_created_and_references_variant(self, tmp_path, offline_builder):
        tables = make_feed(
            tmp_path,
            "route_id,route_short_name,route_long_name,route_type,route_color\n"
            "LIGNE_01,1,Gare - Ecoles,3,fda03f\n",
        )
        builder = offline_builder()
        builder.build_relations(tables)
        builder.build_route_masters(tables)

        [route] = [r for r in builder.relations if r.tags["type"] == "route"]
        [master] = [r for r in builder.relations if r.tags["type"] == "route_master"]
        assert [m.ref for m in master.members] == [route.id]
        assert master.tags["colour"] == "#fda03f"
        assert "None" not in master.tags["name"]


class TestTextIdentifiers:
    """GTFS IDs are text and must keep their leading zeros."""

    def test_stop_ids_keep_leading_zeros(self, tmp_path):
        tables = make_feed(
            tmp_path, "route_id,route_short_name,route_type\nLIGNE_01,1,3\n"
        )
        assert tables["stops"]["stop_id"].to_list() == ["0101", "0102"]
        assert tables["stop_times"]["stop_id"].to_list() == ["0101", "0102"]

    def test_numeric_columns_are_parsed(self, tmp_path):
        tables = make_feed(
            tmp_path, "route_id,route_short_name,route_type\nLIGNE_01,1,3\n"
        )
        assert tables["stops"]["stop_lat"][0] == pytest.approx(44.54607)
        assert tables["stop_times"]["stop_sequence"].to_list() == [1, 2]
        assert tables["routes"]["route_type"][0] == 3


class TestFormatHelpers:
    def test_format_name_empty(self):
        assert format_name(None) == ""
        assert format_name("") == ""

    @pytest.mark.parametrize(
        ("value", "expected"),
        [
            ("FF0000", "#FF0000"),
            ("#fda03f", "#fda03f"),
            ("000", "#000"),
            ("", None),
            (None, None),
            ("red", None),
            ("12345", None),
        ],
    )
    def test_format_colour(self, value, expected):
        assert format_colour(value) == expected


class FakeOverpassResponse:
    status_code = 200

    @staticmethod
    def json():
        return {"elements": []}


class TestMissingStopsWithTextIds:
    """GTFS stops absent from OSM become new nodes, whatever their ID looks like."""

    def test_new_stops_get_distinct_negative_ids(self, monkeypatch):
        import polars as pl

        monkeypatch.setattr(
            "gtfstoosm.convert.requests.post", lambda *a, **k: FakeOverpassResponse()
        )
        stops = pl.DataFrame(
            {
                "stop_id": ["CHOGARE", "0101", "CHOGARE"],
                "lat": [44.54607, 44.5442, 44.54607],
                "lon": [6.27145, 6.27793, 6.27145],
                "name": ["Gare", "Ecoles", "Gare"],
            }
        )
        builder = OSMRelationBuilder()
        elements = builder._get_stop_objects(stops, add_missing_stops=True)

        assert [e.id for e in elements] == [-1, -2, -1]
        assert [n.id for n in builder.new_stops] == [-1, -2]
        assert builder.new_stops[0].tags["name"] == "Gare"
