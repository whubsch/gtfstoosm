import zipfile
from pathlib import Path

import polars as pl
import pytest

from gtfstoosm.gtfs import GTFSFeed, GTFSValidationError


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
    routes_csv = "route_id,route_short_name,route_type\nR1,1,3\n"
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


class TestGTFSValidation:
    """Tests for GTFS feed validation."""

    def test_validate_valid_feed(self, minimal_gtfs_zip):
        """Test validation passes for a valid GTFS feed."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip)
        issues = feed.validate_feed(strict=False)

        # Should have at least one info message
        assert len(issues) > 0
        # Check for success message
        assert any("validation passed" in issue.lower() for issue in issues)
        # Should not have errors
        assert not any(issue.startswith("ERROR:") for issue in issues)

    def test_validate_missing_required_file(self, tmp_path):
        """Test validation fails when required files are missing."""
        zip_path = tmp_path / "incomplete.zip"

        # Create a zip with only some required files
        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            # Missing: routes.txt, trips.txt, stop_times.txt

        feed = GTFSFeed(feed_dir=str(zip_path))
        issues = feed.validate_feed(strict=False)

        # Should have errors about missing files
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert len(error_issues) > 0
        assert any("routes.txt" in issue for issue in error_issues)

    def test_validate_missing_required_file_strict(self, tmp_path):
        """Test validation raises exception in strict mode for missing files."""
        zip_path = tmp_path / "incomplete.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        with pytest.raises(GTFSValidationError, match="Missing required GTFS files"):
            feed.validate_feed(strict=True)

    def test_validate_missing_required_columns(self, tmp_path):
        """Test validation fails when required columns are missing."""
        zip_path = tmp_path / "bad_columns.zip"

        # Create files with missing required columns
        agency_csv = (
            "agency_id,agency_name\n1,Test Agency\n"  # Missing url and timezone
        )
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\n"
        routes_csv = (
            "route_id,route_type\nR1,3\n"  # Has route_type but missing both name fields
        )
        trips_csv = "route_id,service_id,trip_id\nR1,SVC1,T1\n"
        stop_times_csv = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            zf.writestr("routes.txt", routes_csv)
            zf.writestr("trips.txt", trips_csv)
            zf.writestr("stop_times.txt", stop_times_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        issues = feed.validate_feed(strict=False)

        # Should have errors about missing columns
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert len(error_issues) > 0
        assert any(
            "agency.txt" in issue and "Missing required columns" in issue
            for issue in error_issues
        )
        # Check for routes.txt error about missing name fields
        assert any(
            "routes.txt" in issue
            and ("route_short_name" in issue or "route_long_name" in issue)
            for issue in error_issues
        )

    def test_validate_routes_with_only_short_name(self, tmp_path):
        """Test validation passes when routes has only route_short_name."""
        zip_path = tmp_path / "short_name_only.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\n"
        routes_csv = "route_id,route_short_name,route_type\nR1,1,3\n"  # Only short_name
        trips_csv = "route_id,service_id,trip_id\nR1,SVC1,T1\n"
        stop_times_csv = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            zf.writestr("routes.txt", routes_csv)
            zf.writestr("trips.txt", trips_csv)
            zf.writestr("stop_times.txt", stop_times_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        issues = feed.validate_feed(strict=False)

        # Should not have errors about routes
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert not any("routes.txt" in issue for issue in error_issues)

    def test_validate_non_existent_file(self, tmp_path):
        """Test validation handles non-existent files properly."""
        non_existent = tmp_path / "does_not_exist.zip"
        feed = GTFSFeed(feed_dir=str(non_existent))

        with pytest.raises(FileNotFoundError):
            feed.validate_feed(strict=False)

    def test_load_with_validation_disabled(self, minimal_gtfs_zip):
        """Test loading a feed with validation disabled."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip)
        feed.load(validate_feed=False)

        # Should still load successfully
        assert len(feed.tables) > 0
        assert "stops" in feed.tables

    def test_load_with_validation_enabled(self, minimal_gtfs_zip):
        """Test loading a feed with validation enabled."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip)
        feed.load(validate_feed=True, strict=False)

        # Should load successfully
        assert len(feed.tables) > 0

    def test_load_invalid_feed_strict_mode(self, tmp_path):
        """Test loading an invalid feed in strict mode raises exception."""
        zip_path = tmp_path / "incomplete.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        with pytest.raises(GTFSValidationError):
            feed.load(validate_feed=True, strict=True)


class TestReferentialIntegrity:
    """Tests for referential integrity validation."""

    def test_referential_integrity_valid_feed(self, minimal_gtfs_zip):
        """Test referential integrity validation passes for valid feed."""
        feed = GTFSFeed(feed_dir=minimal_gtfs_zip)
        feed.load(validate_feed=False)

        issues = feed.validate_referential_integrity()

        # Should have success message
        assert any("validation passed" in issue.lower() for issue in issues)
        # Should not have errors
        assert not any(issue.startswith("ERROR:") for issue in issues)

    def test_referential_integrity_invalid_route_id(self, tmp_path):
        """Test detection of invalid route_id in trips."""
        zip_path = tmp_path / "bad_refs.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\n"
        routes_csv = "route_id,route_short_name,route_type\nR1,1,3\n"
        trips_csv = "route_id,service_id,trip_id\nR999,SVC1,T1\n"  # Invalid route_id
        stop_times_csv = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S1,1\n"

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            zf.writestr("routes.txt", routes_csv)
            zf.writestr("trips.txt", trips_csv)
            zf.writestr("stop_times.txt", stop_times_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        feed.load(validate_feed=False)

        issues = feed.validate_referential_integrity()

        # Should have error about invalid route_id
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert len(error_issues) > 0
        assert any(
            "route_id" in issue and "invalid references" in issue
            for issue in error_issues
        )

    def test_referential_integrity_invalid_trip_id(self, tmp_path):
        """Test detection of invalid trip_id in stop_times."""
        zip_path = tmp_path / "bad_trip.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\n"
        routes_csv = "route_id,route_short_name,route_type\nR1,1,3\n"
        trips_csv = "route_id,service_id,trip_id\nR1,SVC1,T1\n"
        stop_times_csv = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT999,08:00:00,08:00:00,S1,1\n"  # Invalid trip_id

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            zf.writestr("routes.txt", routes_csv)
            zf.writestr("trips.txt", trips_csv)
            zf.writestr("stop_times.txt", stop_times_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        feed.load(validate_feed=False)

        issues = feed.validate_referential_integrity()

        # Should have error about invalid trip_id
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert len(error_issues) > 0
        assert any(
            "trip_id" in issue and "invalid references" in issue
            for issue in error_issues
        )

    def test_referential_integrity_invalid_stop_id(self, tmp_path):
        """Test detection of invalid stop_id in stop_times."""
        zip_path = tmp_path / "bad_stop.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\nS1,Stop 1,40.7128,-74.0060\n"
        routes_csv = "route_id,route_short_name,route_type\nR1,1,3\n"
        trips_csv = "route_id,service_id,trip_id\nR1,SVC1,T1\n"
        stop_times_csv = "trip_id,arrival_time,departure_time,stop_id,stop_sequence\nT1,08:00:00,08:00:00,S999,1\n"  # Invalid stop_id

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            zf.writestr("routes.txt", routes_csv)
            zf.writestr("trips.txt", trips_csv)
            zf.writestr("stop_times.txt", stop_times_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        feed.load(validate_feed=False)

        issues = feed.validate_referential_integrity()

        # Should have error about invalid stop_id
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert len(error_issues) > 0
        assert any(
            "stop_id" in issue and "invalid references" in issue
            for issue in error_issues
        )

    def test_referential_integrity_empty_tables(self, tmp_path):
        """Test detection of empty required tables."""
        zip_path = tmp_path / "empty_tables.zip"

        agency_csv = "agency_id,agency_name,agency_url,agency_timezone\n1,Test,http://test.com,America/New_York\n"
        stops_csv = "stop_id,stop_name,stop_lat,stop_lon\n"  # Empty
        routes_csv = "route_id,route_short_name,route_type\n"  # Empty
        trips_csv = "route_id,service_id,trip_id\n"
        stop_times_csv = (
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"  # Empty
        )

        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("agency.txt", agency_csv)
            zf.writestr("stops.txt", stops_csv)
            zf.writestr("routes.txt", routes_csv)
            zf.writestr("trips.txt", trips_csv)
            zf.writestr("stop_times.txt", stop_times_csv)

        feed = GTFSFeed(feed_dir=str(zip_path))
        feed.load(validate_feed=False)

        issues = feed.validate_referential_integrity()

        # Should have errors about empty tables
        error_issues = [i for i in issues if i.startswith("ERROR:")]
        assert len(error_issues) > 0
        assert any("empty" in issue.lower() for issue in error_issues)

    def test_referential_integrity_before_load(self):
        """Test that validation warns if called before load()."""
        feed = GTFSFeed(feed_dir="dummy.zip")
        issues = feed.validate_referential_integrity()

        # Should have warning about no tables loaded
        assert len(issues) > 0
        assert any("No tables loaded" in issue for issue in issues)
