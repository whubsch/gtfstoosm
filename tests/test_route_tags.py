"""Tests for the tags added to route relations (offline, network stubbed)."""

import pytest

from gtfstoosm.cli import main
from gtfstoosm.convert import OSMRelationBuilder
from tests.test_sparse_feeds import make_feed

ROUTES_CSV = (
    "route_id,route_short_name,route_long_name,route_type,route_color\n"
    "LIGNE_01,1,Gare - Ecoles,3,fda03f\n"
)


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(
        OSMRelationBuilder, "_get_stop_objects", lambda self, *a, **k: []
    )
    monkeypatch.setattr(OSMRelationBuilder, "_get_route_ways", lambda self, *a, **k: [])


def build(tmp_path, **kwargs):
    tables = make_feed(tmp_path, ROUTES_CSV)
    builder = OSMRelationBuilder(**kwargs)
    builder.build_relations(tables)
    builder.build_route_masters(tables)
    route = next(r for r in builder.relations if r.tags["type"] == "route")
    master = next(r for r in builder.relations if r.tags["type"] == "route_master")
    return route, master


def test_from_and_to_are_terminus_names(tmp_path):
    route, _ = build(tmp_path)
    assert route.tags["from"] == "Gare"
    assert route.tags["to"] == "Ecoles"


def test_default_name_is_unchanged(tmp_path):
    route, _ = build(tmp_path)
    assert route.tags["name"] == "Route 1 Gare-Ecoles"


def test_ptv2_names(tmp_path):
    route, _ = build(tmp_path, ptv2_names=True)
    assert route.tags["name"] == "bus 1: Gare => Ecoles"


def test_no_gtfs_tags_without_feed_id(tmp_path):
    route, master = build(tmp_path)
    for relation in (route, master):
        assert not [k for k in relation.tags if k.startswith("gtfs:")]


def test_gtfs_tags_with_feed_id(tmp_path):
    route, master = build(tmp_path, feed_id="FR-TEST")
    assert route.tags["gtfs:feed"] == "FR-TEST"
    assert route.tags["gtfs:route_id"] == "LIGNE_01"
    assert route.tags["gtfs:trip_id:sample"] == "T1"
    assert master.tags["gtfs:feed"] == "FR-TEST"
    assert master.tags["gtfs:route_id"] == "LIGNE_01"


def test_master_ref_matches_variants(tmp_path):
    route, master = build(tmp_path)
    assert master.tags["ref"] == route.tags["ref"] == "1"


def test_cli_passes_feed_id_and_ptv2_names(tmp_path):
    make_feed(tmp_path, ROUTES_CSV)
    out = tmp_path / "cli.osc"
    code = main(
        [
            "-i",
            str(tmp_path / "sparse_gtfs.zip"),
            "-o",
            str(out),
            "--feed-id",
            "FR-TEST",
            "--ptv2-names",
        ]
    )
    assert code == 0
    content = out.read_text(encoding="utf8")
    assert 'k="gtfs:feed" v="FR-TEST"' in content
    assert "bus 1: Gare =&gt; Ecoles" in content or "bus 1: Gare => Ecoles" in content
