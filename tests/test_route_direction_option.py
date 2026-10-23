"""convert_gtfs_to_osm must honour the documented add_route_direction option."""

import zipfile

import pytest

from gtfstoosm.convert import OSMRelationBuilder, convert_gtfs_to_osm


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    monkeypatch.setattr(
        OSMRelationBuilder, "_get_stop_objects", lambda self, *a, **k: []
    )
    monkeypatch.setattr(OSMRelationBuilder, "_get_route_ways", lambda self, *a, **k: [])


@pytest.fixture
def gtfs_zip(tmp_path):
    files = {
        "agency.txt": "agency_id,agency_name,agency_url,agency_timezone\n"
        "A1,Agency,http://example.org,Europe/Paris\n",
        "stops.txt": "stop_id,stop_name,stop_lat,stop_lon\n"
        "1,South,40.0,-74.0\n2,North,41.0,-74.0\n",
        "routes.txt": "route_id,agency_id,route_short_name,route_long_name,"
        "route_desc,route_type,route_url,route_color,route_text_color\n"
        "R1,A1,L1,One,,3,,FF0000,FFFFFF\n",
        "trips.txt": "route_id,service_id,trip_id,shape_id\nR1,S1,T1,SH1\n",
        "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "T1,08:00:00,08:00:00,1,1\nT1,08:10:00,08:10:00,2,2\n",
        "shapes.txt": "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\n"
        "SH1,40.0,-74.0,1\nSH1,41.0,-74.0,2\n",
    }
    path = tmp_path / "gtfs.zip"
    with zipfile.ZipFile(path, "w") as zf:
        for name, content in files.items():
            zf.writestr(name, content)
    return path


def convert(gtfs_zip, tmp_path, **options):
    out = tmp_path / "out.osc"
    convert_gtfs_to_osm(str(gtfs_zip), str(out), **options)
    return out.read_text(encoding="utf8")


def test_add_route_direction_adds_direction_to_name(gtfs_zip, tmp_path):
    with_direction = convert(gtfs_zip, tmp_path, add_route_direction=True)
    without = convert(gtfs_zip, tmp_path)
    assert with_direction != without
    assert 'v="Route L1 One Northbound"' in with_direction


def test_legacy_route_direction_option_still_works(gtfs_zip, tmp_path):
    assert 'v="Route L1 One Northbound"' in convert(
        gtfs_zip, tmp_path, route_direction=True
    )
