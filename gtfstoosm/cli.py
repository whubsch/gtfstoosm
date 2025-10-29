"""
Command-line interface for the GTFS to OSM converter.

This module provides a CLI for converting GTFS feeds to OSM relations.
"""

import argparse
import logging
import os
import sys

from gtfstoosm.convert import convert_gtfs_to_osm
from gtfstoosm.utils import parse_tag_string


def setup_logging(verbose: bool = False) -> None:
    """
    Set up logging configuration.

    Args:
        verbose: Whether to enable verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def parse_args(args: list[str] | None = None) -> argparse.Namespace:
    """
    Parse command-line arguments.

    Args:
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Convert GTFS transit feeds to OpenStreetMap relations"
    )

    parser.add_argument(
        "--input",
        "-i",
        dest="input_feed",
        required=True,
        help="Path to the GTFS feed zip file",
    )

    parser.add_argument(
        "--output",
        "-o",
        dest="output_file",
        required=True,
        help="Path to write the OSM XML file",
    )

    parser.add_argument(
        "--exclude-stops",
        dest="exclude_stops",
        action="store_true",
        default=False,
        help="Exclude stops from the output",
    )

    parser.add_argument(
        "--exclude-routes",
        dest="exclude_routes",
        action="store_true",
        default=False,
        help="Exclude routes from the output",
    )

    parser.add_argument(
        "--add-missing-stops",
        dest="add_missing_stops",
        action="store_true",
        default=False,
        help="Add stops missing from the database to the output (default: False)",
    )

    # parser.add_argument(
    #     "--route-types",
    #     dest="route_types",
    #     type=int,
    #     nargs="+",
    #     help="Only include routes with these GTFS route_type values (space-separated). Only bus routes are currently supported.",
    # )

    # parser.add_argument(
    #     "--agency",
    #     dest="agency_id",
    #     type=str,
    #     help="Only include routes for this agency ID",
    # )

    parser.add_argument(
        "--stop-search-radius",
        dest="stop_search_radius",
        type=float,
        default=10.0,
        help="Radius to search for stops (in meters)",
    )

    parser.add_argument(
        "--add-route-direction",
        dest="add_route_direction",
        action="store_true",
        default=False,
        help="Add route direction to the output (default: False)",
    )

    parser.add_argument(
        "--route-ref-pattern",
        dest="route_ref_pattern",
        type=str,
        help="Regex pattern to filter routes by route_id (e.g., '^[0-9]+$' for numeric routes only)",
    )

    parser.add_argument(
        "--relation-tags",
        dest="relation_tags",
        type=str,
        help="Semicolon-separated list of tags to add to the route and route_master relations (e.g., 'operator=TransitCenter;network=Whoville Bus;network:wikidata=Q123')",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    return parser.parse_args(args)


def main(args: list[str] | None = None) -> int:
    """
    Main entry point for the CLI.

    Args:
        args: Command-line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parsed_args = parse_args(args)
    setup_logging(parsed_args.verbose)

    logger = logging.getLogger(__name__)
    logger.info("Starting GTFS to OSM conversion")

    try:
        # Validate input file
        if not os.path.exists(parsed_args.input_feed):
            logger.error(f"Input GTFS feed not found: {parsed_args.input_feed}")
            return 1

        # Validate add missing stops
        if parsed_args.add_missing_stops and parsed_args.exclude_stops:
            logger.error("Cannot add missing stops without including stops")
            return 1

        # Validate something is included
        if parsed_args.exclude_stops and parsed_args.exclude_routes:
            logger.error("Nothing to convert")
            return 1

        if parsed_args.stop_search_radius < 0:
            logger.error("Stop search radius must be a positive number")
            return 1

        if parsed_args.stop_search_radius > 10:
            logger.warning("Stop search radius is too large, reverting to 10 meters")
            parsed_args.stop_search_radius = 10

        # Validate output directory
        output_dir = os.path.dirname(parsed_args.output_file)
        if output_dir and not os.path.exists(output_dir):
            logger.info(f"Creating output directory: {output_dir}")
            os.makedirs(output_dir, exist_ok=True)

        # Convert GTFS to OSM
        options = {
            "exclude_stops": parsed_args.exclude_stops,
            "exclude_routes": parsed_args.exclude_routes,
            "add_missing_stops": parsed_args.add_missing_stops,
            "stop_search_radius": parsed_args.stop_search_radius,
            "add_route_direction": parsed_args.add_route_direction,
            "route_ref_pattern": parsed_args.route_ref_pattern,
            "relation_tags": parse_tag_string(parsed_args.relation_tags)
            if parsed_args.relation_tags
            else None,
            # "route_types": parsed_args.route_types,
            # "agency_id": parsed_args.agency_id,
        }

        logger.debug(f"CLI options: {options}")

        convert_gtfs_to_osm(parsed_args.input_feed, parsed_args.output_file, **options)

        logger.info("Conversion completed successfully")
        logger.info(f"Output OSM file: {parsed_args.output_file}")
        return 0

    except Exception as e:
        logger.error(f"Conversion failed: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
