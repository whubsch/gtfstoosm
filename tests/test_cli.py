"""Tests for the CLI module."""

import logging
import sys
from unittest.mock import patch

import pytest

from gtfstoosm.cli import main, parse_args, setup_logging


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_setup_logging_info_level(self, caplog):
        """Test that logging is set to INFO by default."""
        with caplog.at_level(logging.INFO):
            setup_logging(verbose=False)
            logger = logging.getLogger(__name__)

            # Verify INFO level messages are captured
            logger.info("Test info message")
            assert "Test info message" in caplog.text

            # Verify DEBUG messages are not captured at INFO level
            logger.debug("Test debug message")
            assert "Test debug message" not in caplog.text

    def test_setup_logging_debug_level(self, caplog):
        """Test that logging is set to DEBUG when verbose=True."""
        with caplog.at_level(logging.DEBUG):
            setup_logging(verbose=True)
            logger = logging.getLogger(__name__)

            # Verify both DEBUG and INFO messages are captured
            logger.debug("Test debug message")
            logger.info("Test info message")
            assert "Test debug message" in caplog.text
            assert "Test info message" in caplog.text

    def test_setup_logging_format(self, caplog):
        """Test that logging format includes expected components."""
        with caplog.at_level(logging.INFO):
            setup_logging(verbose=False)
            logger = logging.getLogger(__name__)
            logger.info("Test message")

            # The format should include timestamp, name, level, and message
            # We can't test exact format due to timestamp, but we can verify components exist
            assert "INFO" in caplog.text
            assert "Test message" in caplog.text


class TestParseArgs:
    """Tests for parse_args function."""

    def test_parse_args_minimal(self):
        """Test parsing minimal required arguments."""
        args = parse_args(["--input", "input.zip", "--output", "output.osm"])
        assert args.input_feed == "input.zip"
        assert args.output_file == "output.osm"
        assert args.exclude_stops is False
        assert args.exclude_routes is False
        assert args.add_missing_stops is False
        assert args.stop_search_radius == 10.0
        assert args.add_route_direction is False
        assert args.route_ref_pattern is None
        assert args.relation_tags is None
        assert args.verbose is False

    def test_parse_args_short_options(self):
        """Test parsing with short option flags."""
        args = parse_args(["-i", "input.zip", "-o", "output.osm"])
        assert args.input_feed == "input.zip"
        assert args.output_file == "output.osm"

    def test_parse_args_missing_input(self):
        """Test that missing input argument raises error."""
        with pytest.raises(SystemExit):
            parse_args(["--output", "output.osm"])

    def test_parse_args_missing_output(self):
        """Test that missing output argument raises error."""
        with pytest.raises(SystemExit):
            parse_args(["--input", "input.zip"])

    def test_parse_args_exclude_stops(self):
        """Test exclude-stops flag."""
        args = parse_args(
            ["--input", "input.zip", "--output", "output.osm", "--exclude-stops"]
        )
        assert args.exclude_stops is True

    def test_parse_args_exclude_routes(self):
        """Test exclude-routes flag."""
        args = parse_args(
            ["--input", "input.zip", "--output", "output.osm", "--exclude-routes"]
        )
        assert args.exclude_routes is True

    def test_parse_args_add_missing_stops(self):
        """Test add-missing-stops flag."""
        args = parse_args(
            ["--input", "input.zip", "--output", "output.osm", "--add-missing-stops"]
        )
        assert args.add_missing_stops is True

    def test_parse_args_stop_search_radius(self):
        """Test custom stop search radius."""
        args = parse_args(
            [
                "--input",
                "input.zip",
                "--output",
                "output.osm",
                "--stop-search-radius",
                "15.5",
            ]
        )
        assert args.stop_search_radius == 15.5

    def test_parse_args_add_route_direction(self):
        """Test add-route-direction flag."""
        args = parse_args(
            ["--input", "input.zip", "--output", "output.osm", "--add-route-direction"]
        )
        assert args.add_route_direction is True

    def test_parse_args_route_ref_pattern(self):
        """Test route-ref-pattern argument."""
        args = parse_args(
            [
                "--input",
                "input.zip",
                "--output",
                "output.osm",
                "--route-ref-pattern",
                "^[0-9]+$",
            ]
        )
        assert args.route_ref_pattern == "^[0-9]+$"

    def test_parse_args_relation_tags(self):
        """Test relation-tags argument."""
        args = parse_args(
            [
                "--input",
                "input.zip",
                "--output",
                "output.osm",
                "--relation-tags",
                "operator=Test;network=TestNet",
            ]
        )
        assert args.relation_tags == "operator=Test;network=TestNet"

    def test_parse_args_verbose(self):
        """Test verbose flag."""
        args = parse_args(
            ["--input", "input.zip", "--output", "output.osm", "--verbose"]
        )
        assert args.verbose is True

    def test_parse_args_verbose_short(self):
        """Test verbose flag with short option."""
        args = parse_args(["--input", "input.zip", "--output", "output.osm", "-v"])
        assert args.verbose is True

    def test_parse_args_all_options(self):
        """Test parsing all options together."""
        args = parse_args(
            [
                "-i",
                "input.zip",
                "-o",
                "output.osm",
                "--exclude-stops",
                "--add-route-direction",
                "--stop-search-radius",
                "20.0",
                "--route-ref-pattern",
                "^C",
                "--relation-tags",
                "operator=Test",
                "-v",
            ]
        )
        assert args.input_feed == "input.zip"
        assert args.output_file == "output.osm"
        assert args.exclude_stops is True
        assert args.add_route_direction is True
        assert args.stop_search_radius == 20.0
        assert args.route_ref_pattern == "^C"
        assert args.relation_tags == "operator=Test"
        assert args.verbose is True


class TestMain:
    """Tests for main function."""

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_success(self, mock_convert, tmp_path):
        """Test successful conversion."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = ["--input", str(input_file), "--output", str(output_file)]

        exit_code = main(args)

        assert exit_code == 0
        mock_convert.assert_called_once()
        call_args = mock_convert.call_args
        assert call_args[0][0] == str(input_file)
        assert call_args[0][1] == str(output_file)

    def test_main_input_not_found(self, tmp_path, caplog):
        """Test error when input file doesn't exist."""
        output_file = tmp_path / "output.osm"

        args = ["--input", "nonexistent.zip", "--output", str(output_file)]

        with caplog.at_level(logging.ERROR):
            exit_code = main(args)

        assert exit_code == 1
        assert "Input GTFS feed not found" in caplog.text

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_conflicting_options_missing_stops(
        self, mock_convert, tmp_path, caplog
    ):
        """Test error with conflicting add-missing-stops and exclude-stops options."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = [
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--exclude-stops",
            "--add-missing-stops",
        ]

        with caplog.at_level(logging.ERROR):
            exit_code = main(args)

        assert exit_code == 1
        assert "Cannot add missing stops without including stops" in caplog.text
        mock_convert.assert_not_called()

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_nothing_to_convert(self, mock_convert, tmp_path, caplog):
        """Test error when both stops and routes are excluded."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = [
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--exclude-stops",
            "--exclude-routes",
        ]

        with caplog.at_level(logging.ERROR):
            exit_code = main(args)

        assert exit_code == 1
        assert "Nothing to convert" in caplog.text
        mock_convert.assert_not_called()

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_negative_search_radius(self, mock_convert, tmp_path, caplog):
        """Test error with negative stop search radius."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = [
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--stop-search-radius",
            "-5",
        ]

        with caplog.at_level(logging.ERROR):
            exit_code = main(args)

        assert exit_code == 1
        assert "Stop search radius must be a positive number" in caplog.text
        mock_convert.assert_not_called()

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_large_search_radius_warning(self, mock_convert, tmp_path, caplog):
        """Test warning and clamping of large stop search radius."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = [
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--stop-search-radius",
            "50",
        ]

        with caplog.at_level(logging.WARNING):
            exit_code = main(args)

        assert exit_code == 0
        assert "Stop search radius is too large, reverting to 10 meters" in caplog.text

        # Verify the radius was clamped to 10
        call_kwargs = mock_convert.call_args[1]
        assert call_kwargs["stop_search_radius"] == 10

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_creates_output_directory(self, mock_convert, tmp_path, caplog):
        """Test that output directory is created if it doesn't exist."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_dir = tmp_path / "new_dir" / "subdir"
        output_file = output_dir / "output.osm"

        args = ["--input", str(input_file), "--output", str(output_file)]

        with caplog.at_level(logging.INFO):
            exit_code = main(args)

        assert exit_code == 0
        assert output_dir.exists()
        assert f"Creating output directory: {output_dir}" in caplog.text

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_passes_all_options(self, mock_convert, tmp_path):
        """Test that all options are passed to convert function."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = [
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--exclude-stops",
            "--add-route-direction",
            "--stop-search-radius",
            "8.5",
            "--route-ref-pattern",
            "^[0-9]+$",
            "--relation-tags",
            "operator=Test;network=TestNet",
        ]

        exit_code = main(args)

        assert exit_code == 0
        mock_convert.assert_called_once()

        call_kwargs = mock_convert.call_args[1]
        assert call_kwargs["exclude_stops"] is True
        assert call_kwargs["exclude_routes"] is False
        assert call_kwargs["add_missing_stops"] is False
        assert call_kwargs["add_route_direction"] is True
        assert call_kwargs["stop_search_radius"] == 8.5
        assert call_kwargs["route_ref_pattern"] == "^[0-9]+$"
        assert call_kwargs["relation_tags"] == {
            "operator": "Test",
            "network": "TestNet",
        }

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_handles_conversion_exception(self, mock_convert, tmp_path, caplog):
        """Test error handling when conversion raises exception."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        mock_convert.side_effect = ValueError("Test error")

        args = ["--input", str(input_file), "--output", str(output_file)]

        with caplog.at_level(logging.ERROR):
            exit_code = main(args)

        assert exit_code == 1
        assert "Conversion failed: Test error" in caplog.text

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_with_verbose_logging(self, mock_convert, tmp_path, caplog):
        """Test that verbose flag enables debug logging."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = ["--input", str(input_file), "--output", str(output_file), "--verbose"]

        with caplog.at_level(logging.DEBUG):
            exit_code = main(args)

        assert exit_code == 0
        assert "CLI options:" in caplog.text

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_output_file_same_directory(self, mock_convert, tmp_path):
        """Test output file in same directory as input (no subdirectory)."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = "output.osm"  # No directory component

        args = ["--input", str(input_file), "--output", output_file]

        exit_code = main(args)

        assert exit_code == 0
        mock_convert.assert_called_once()

    def test_main_with_none_args_uses_sys_argv(self, tmp_path):
        """Test that main() without args uses sys.argv."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        test_argv = [
            "gtfstoosm",
            "--input",
            str(input_file),
            "--output",
            str(output_file),
        ]

        with patch.object(sys, "argv", test_argv):
            with patch("gtfstoosm.cli.convert_gtfs_to_osm"):
                exit_code = main()

        assert exit_code == 0

    @patch("gtfstoosm.cli.convert_gtfs_to_osm")
    def test_main_with_relation_tags_parsing(self, mock_convert, tmp_path):
        """Test that relation tags are properly parsed."""
        input_file = tmp_path / "input.zip"
        input_file.touch()
        output_file = tmp_path / "output.osm"

        args = [
            "--input",
            str(input_file),
            "--output",
            str(output_file),
            "--relation-tags",
            "operator=Transit;network=City Bus;network:wikidata=Q123",
        ]

        exit_code = main(args)

        assert exit_code == 0
        call_kwargs = mock_convert.call_args[1]
        assert call_kwargs["relation_tags"] == {
            "operator": "Transit",
            "network": "City Bus",
            "network:wikidata": "Q123",
        }
