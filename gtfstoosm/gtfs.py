"""
GTFS data handling module.

This module provides functionality for parsing and processing GTFS data.
It handles the reading and validation of GTFS feeds.
"""

import logging
import zipfile
from io import BytesIO

import polars as pl
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class GTFSValidationError(Exception):
    """Exception raised when GTFS feed validation fails."""

    pass


class GTFSFeed(BaseModel):
    """Class for storing and querying a GTFS feed."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,  # Allow Polars DataFrames
        validate_assignment=True,
    )

    feed_dir: str
    tables: dict[str, pl.DataFrame] = Field(default_factory=dict)
    name: str | None = None
    required_files: list[str] = Field(
        default_factory=lambda: [
            "agency.txt",
            "stops.txt",
            "routes.txt",
            "trips.txt",
            "stop_times.txt",
        ]
    )
    optional_files: list[str] = Field(
        default_factory=lambda: [
            "shapes.txt",
            "calendar.txt",
            "calendar_dates.txt",
        ]
    )

    # Define required columns for each file (only for files needed for OSM conversion)
    required_columns: dict[str, list[str]] = Field(
        default_factory=lambda: {
            "agency": ["agency_name", "agency_url", "agency_timezone"],
            "stops": ["stop_id", "stop_name", "stop_lat", "stop_lon"],
            "routes": [
                "route_id",
                "route_type",
            ],  # route_short_name OR route_long_name needed but not both
            "trips": ["route_id", "service_id", "trip_id"],
            "stop_times": [
                "trip_id",
                "arrival_time",
                "departure_time",
                "stop_id",
                "stop_sequence",
            ],
            "shapes": [
                "shape_id",
                "shape_pt_lat",
                "shape_pt_lon",
                "shape_pt_sequence",
            ],
        }
    )

    def validate_feed(self, strict: bool = False) -> list[str]:
        """
        Validate the GTFS feed structure and contents.

        Args:
            strict: If True, raises GTFSValidationError on any validation failure.
                   If False, returns a list of validation warnings/errors.

        Returns:
            List of validation messages (warnings and errors)

        Raises:
            GTFSValidationError: If strict=True and validation fails
            FileNotFoundError: If the feed file doesn't exist
            zipfile.BadZipFile: If the feed file is not a valid zip
        """
        issues: list[str] = []

        try:
            # Check if feed file exists and is a valid zip
            with zipfile.ZipFile(self.feed_dir, "r") as zip_ref:
                available_files = set(zip_ref.namelist())

                # Validate required files are present
                missing_files: list[str] = []
                for required_file in self.required_files:
                    if required_file not in available_files:
                        missing_files.append(required_file)

                if missing_files:
                    error_msg = (
                        f"Missing required GTFS files: {', '.join(missing_files)}"
                    )
                    issues.append(f"ERROR: {error_msg}")
                    if strict:
                        raise GTFSValidationError(error_msg)

                # Validate file structure (columns) for available files
                for file in available_files:
                    if not file.endswith(".txt"):
                        continue

                    table_name = file[:-4]  # Remove .txt extension

                    # Skip files we don't have column requirements for
                    if table_name not in self.required_columns:
                        continue

                    try:
                        with zip_ref.open(file) as file_obj:
                            # Read just the header to check columns
                            df = pl.read_csv(
                                BytesIO(file_obj.read()),
                                infer_schema_length=None,
                                n_rows=1,
                            )

                            # Check for required columns
                            available_columns = set(df.columns)
                            required_cols = set(self.required_columns[table_name])
                            missing_columns = required_cols - available_columns

                            if missing_columns:
                                error_msg = f"{file}: Missing required columns: {', '.join(sorted(missing_columns))}"
                                issues.append(f"ERROR: {error_msg}")
                                if strict:
                                    raise GTFSValidationError(error_msg)

                            # Special check for routes: need at least one name field
                            if table_name == "routes":
                                if (
                                    "route_short_name" not in available_columns
                                    and "route_long_name" not in available_columns
                                ):
                                    error_msg = f"{file}: Must have either route_short_name or route_long_name"
                                    issues.append(f"ERROR: {error_msg}")
                                    if strict:
                                        raise GTFSValidationError(error_msg)

                    except Exception as e:
                        error_msg = f"{file}: Failed to read file - {str(e)}"
                        issues.append(f"ERROR: {error_msg}")
                        if strict:
                            raise GTFSValidationError(error_msg) from e

                # Add success message if no errors found (warnings are ok)
                has_errors = any(issue.startswith("ERROR:") for issue in issues)
                if not has_errors:
                    issues.append("INFO: Basic GTFS structure validation passed")

        except FileNotFoundError as e:
            error_msg = f"GTFS feed file not found: {self.feed_dir}"
            issues.append(f"ERROR: {error_msg}")
            if strict:
                raise GTFSValidationError(error_msg) from e
            raise

        except zipfile.BadZipFile as e:
            error_msg = f"Invalid zip file: {self.feed_dir}"
            issues.append(f"ERROR: {error_msg}")
            if strict:
                raise GTFSValidationError(error_msg) from e
            raise

        return issues

    def validate_referential_integrity(self) -> list[str]:
        """
        Validate referential integrity between GTFS tables.

        This checks that foreign key relationships are valid:
        - trips.route_id references routes.route_id
        - stop_times.trip_id references trips.trip_id
        - stop_times.stop_id references stops.stop_id
        - etc.

        Returns:
            List of referential integrity issues found

        Note:
            This should be called after load() has been called.
        """
        issues: list[str] = []

        if not self.tables:
            issues.append(
                "WARNING: No tables loaded. Call load() before validating referential integrity."
            )
            return issues

        # Check trips.route_id references routes.route_id
        if "trips" in self.tables and "routes" in self.tables:
            route_ids = set(self.tables["routes"]["route_id"].to_list())
            trip_route_ids = set(self.tables["trips"]["route_id"].to_list())
            invalid_routes = trip_route_ids - route_ids

            if invalid_routes:
                issues.append(
                    f"ERROR: trips.route_id contains {len(invalid_routes)} invalid references to routes.route_id"
                )

        # Check stop_times.trip_id references trips.trip_id
        if "stop_times" in self.tables and "trips" in self.tables:
            trip_ids = set(self.tables["trips"]["trip_id"].to_list())
            stop_time_trip_ids = set(self.tables["stop_times"]["trip_id"].to_list())
            invalid_trips = stop_time_trip_ids - trip_ids

            if invalid_trips:
                issues.append(
                    f"ERROR: stop_times.trip_id contains {len(invalid_trips)} invalid references to trips.trip_id"
                )

        # Check stop_times.stop_id references stops.stop_id
        if "stop_times" in self.tables and "stops" in self.tables:
            stop_ids = set(self.tables["stops"]["stop_id"].to_list())
            stop_time_stop_ids = set(self.tables["stop_times"]["stop_id"].to_list())
            invalid_stops = stop_time_stop_ids - stop_ids

            if invalid_stops:
                issues.append(
                    f"ERROR: stop_times.stop_id contains {len(invalid_stops)} invalid references to stops.stop_id"
                )

        # Check shapes reference if trips use shape_id
        if "trips" in self.tables and "shapes" in self.tables:
            if "shape_id" in self.tables["trips"].columns:
                shape_ids = set(self.tables["shapes"]["shape_id"].to_list())
                # Filter out null/empty shape_ids
                trip_shape_ids = set(
                    self.tables["trips"]
                    .filter(pl.col("shape_id").is_not_null())["shape_id"]
                    .to_list()
                )
                invalid_shapes = trip_shape_ids - shape_ids

                if invalid_shapes:
                    issues.append(
                        f"ERROR: trips.shape_id contains {len(invalid_shapes)} invalid references to shapes.shape_id"
                    )

        # Check data completeness
        if "stop_times" in self.tables:
            stop_times_count = self.tables["stop_times"].height
            if stop_times_count == 0:
                issues.append("ERROR: stop_times.txt is empty")

        if "stops" in self.tables:
            stops_count = self.tables["stops"].height
            if stops_count == 0:
                issues.append("ERROR: stops.txt is empty")

        if "routes" in self.tables:
            routes_count = self.tables["routes"].height
            if routes_count == 0:
                issues.append("ERROR: routes.txt is empty")

        if not issues:
            issues.append("INFO: Referential integrity validation passed")

        return issues

    def load(self, validate_feed: bool = True, strict: bool = False) -> None:
        """
        Load a GTFS feed.

        Args:
            validate_feed: If True, validates the feed structure before loading
            strict: If True, raises exception on validation failures

        Raises:
            GTFSValidationError: If validation fails and strict=True
            FileNotFoundError: If the feed file doesn't exist
            zipfile.BadZipFile: If the feed file is not a valid zip
        """
        # Validate feed structure first
        if validate_feed:
            validation_issues = self.validate_feed(strict=strict)

            # Log validation results
            for issue in validation_issues:
                if issue.startswith("ERROR:"):
                    logger.error(issue)
                elif issue.startswith("WARNING:"):
                    logger.warning(issue)
                else:
                    logger.info(issue)

            # Check if there were any errors
            has_errors = any(issue.startswith("ERROR:") for issue in validation_issues)
            if has_errors and strict:
                raise GTFSValidationError(
                    "Feed validation failed. See logs for details."
                )

        # Read all files in the feed directory
        try:
            with zipfile.ZipFile(self.feed_dir, "r") as zip_ref:
                all_txt_files = [f for f in zip_ref.namelist() if f.endswith(".txt")]

                for file in all_txt_files:
                    table_name = file[:-4]  # Remove the .txt extension

                    # Skip if not in required or optional files
                    if (
                        file not in self.required_files
                        and file not in self.optional_files
                    ):
                        logger.debug(f"Skipping unknown file {file}")
                        continue

                    try:
                        logger.debug(f"Loading {file}")
                        with zip_ref.open(file) as file_obj:
                            # Read the CSV data into a polars DataFrame
                            df = pl.read_csv(
                                BytesIO(file_obj.read()), infer_schema_length=None
                            )

                            logger.info(f"Loaded {df.height:,} records from {file}")
                            self.tables[table_name] = df

                    except Exception as e:
                        logger.error(f"Failed to load {file}: {str(e)}")
                        if strict:
                            raise

                # Check if we loaded required files
                loaded_required = [
                    f for f in self.required_files if f[:-4] in self.tables
                ]
                if len(loaded_required) < len(self.required_files):
                    missing = [
                        f for f in self.required_files if f[:-4] not in self.tables
                    ]
                    error_msg = f"Failed to load required files: {', '.join(missing)}"
                    logger.error(error_msg)
                    if strict:
                        raise GTFSValidationError(error_msg)

        except zipfile.BadZipFile as err:
            raise ValueError(
                f"The file at {self.feed_dir} is not a valid zip file"
            ) from err
