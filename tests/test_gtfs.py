import zipfile
from pathlib import Path

import polars as pl
import pytest

from gtfstoosm.gtfs import GTFSFeed


@pytest.fixture
def fixtures_dir():
    """Return the path to the fixtures directory."""
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_gtfs_zip(fixtures_dir):
    """Return the path to the sample GTFS zip file."""
    zip_path = fixtures_dir / "omniride.zip"
    if not zip_path.exists():
        pytest.skip(f"Sample GTFS zip file not found at {zip_path}")
    return str(zip_path)


@pytest.fixture
def minimal_gtfs_zip(tmp_path):
    """Create a minimal GTFS zip file for testing."""
    zip_path = tmp_path / "minimal_gtfs.zip"

    # Create minimal GTFS files
    agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test Agency,http://test.com,America/New_York\n"
    stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\nS2,Stop 2,40.7228,-74.0160\n"
    routes_csv = (
        "route_id,route_short_name,route_long_name,route_type\nR1,1,Route One,3\n"
    )
    trips_csv = "route_id,service_id,trip_id\nR1,SVC1,T1\n"
    stop_times_csv = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\nT1,08:10:00,08:10:00,S2,2\n"
    shapes_csv = "shape_id,shape_pt_lat,shape_pt_lon,shape_pt_sequence\nSHP1,40.7128,-74.0060,1\nSHP1,40.7228,-74.0160,2\n"

    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("agency.txt", agency_csv)
        zf.writestr("stops.txt", stops_csv)
        zf.writestr("routes.txt", routes_csv)
        zf.writestr("trips.txt", trips_csv)
        zf.writestr("stop_times.txt", stop_times_csv)
        zf.writestr("shapes.txt", shapes_csv)

    return str(zip_path)


class TestGTFSFeed:
    """Tests for the GTFSFeed class."""

    def test_gtfs_feed_with_name(self, minimal_gtfs_zip):
        """Test GTFSFeed initialization with a name."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip, name="Test Feed")
        assert feed.name == "Test Feed"

    def test_load_minimal_feed(self, minimal_gtfs_zip):
        """Test loading a minimal GTFS feed."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip)
        feed.load()

        # Check that all required tables are loaded
        assert "agency" in feed.tables
        assert "stops" in feed.tables
        assert "routes" in feed.tables
        assert "trips" in feed.tables
        assert "stop_times" in feed.tables
        assert "shapes" in feed.tables

        # Verify data is loaded correctly
        assert isinstance(feed.tables["agency"], pl.DataFrame)
        assert feed.tables["stops"].height == 2
        assert feed.tables["routes"].height == 1

    def test_load_sample_feed(self, sample_gtfs_zip):
        """Test loading the sample GTFS feed from fixtures."""
        feed = GTFSFeed(feed_dir=sample_gtfs_zip)
        feed.load()

        # Verify tables are loaded
        assert len(feed.tables) > 0
        assert "stops" in feed.tables or "agency" in feed.tables

    def test_load_invalid_zip(self, tmp_path):
        """Test loading an invalid zip file."""
        invalid_zip = tmp_path / "invalid.zip"
        invalid_zip.write_text("This is not a zip file")

        feed = GTFSFeed(feed_dir=str(invalid_zip))
        with pytest.raises(zipfile.BadZipFile, match="not a zip file"):
            feed.load()

    def test_load_missing_file(self, tmp_path):
        """Test loading a non-existent file."""
        non_existent = tmp_path / "does_not_exist.zip"
        feed = GTFSFeed(feed_dir=str(non_existent))

        with pytest.raises(FileNotFoundError):
            feed.load()

    def test_tables_are_dataframes(self, minimal_gtfs_zip):
        """Test that all loaded tables are Polars DataFrames."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip)
        feed.load()

        for table_name, table_df in feed.tables.items():
            assert isinstance(table_df, pl.DataFrame), (
                f"{table_name} is not a DataFrame"
            )
