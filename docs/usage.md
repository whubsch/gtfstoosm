# GTFS to OSM Converter Usage Guide

This guide explains how to use the GTFS to OSM converter to transform public transit data from the General Transit Feed Specification (GTFS) format into OpenStreetMap (OSM) relations.

## Basic Usage

The converter can be used as a Python library.

### Command-Line Interface

The basic syntax for the command-line interface is:

```bash
python -m gtfstoosm.cli --input /path/to/gtfs.zip --output output.osc
```

### Required Arguments

- `--input`, `-i`: Path to the input GTFS feed zip file
- `--output`, `-o`: Path where the output OSM XML file will be written

### Optional Arguments

#### Output Control

- `--exclude-stops`: Exclude stops from the output (default: False)
- `--exclude-routes`: Exclude routes from the output (default: False)
- `--add-missing-stops`: Add stops that are missing from the OSM database to the output (default: False)
  - Note: Cannot be used with `--exclude-stops`

#### Route Filtering

- `--route-ref-pattern`: Regex pattern to filter routes by their `route_id`. This allows you to process only specific routes that match the pattern. Note that most GTFS feeds use the route reference code as the `route_id`, but some use a random integer.
  - Example: `"^[0-9]+$"` - Only numeric route IDs
  - Example: `"^C"` - Only routes starting with 'C'
  - Example: `"^(10|20|30)$"` - Only routes 10, 20, or 30

#### Stop Matching

- `--stop-search-radius`: Radius in meters to search for existing OSM stops (default: 10.0)
  - Decrease for more precise matching in dense areas
  - Any value above 10 meters will be ignored because it will take too long to search

#### Route Options

- `--add-route-direction`: Add route direction information to the output (default: False)
  - Adds directional tags to help distinguish route variants

#### Logging

- `--verbose`, `-v`: Enable verbose (DEBUG level) logging for troubleshooting

## Examples

### Basic Conversion

Convert an entire GTFS feed to OSM:

```bash
python -m gtfstoosm.cli -i transit_agency.zip -o transit_routes.osc
```

### Converting Only Some Routes

Only the routes whose `route_id` matches a regular expression, tagged with their network:

```bash
python -m gtfstoosm.cli -i regional_transit.zip -o city_bus.osc     --route-ref-pattern '^(10|20|30)$'     --relation-tags 'network=City Bus;operator=City Transit'
```

### With Docker

The feed and the result live in the `data` folder (see the README):

```bash
docker compose run --rm convert -i /data/transit_agency.zip -o /data/transit_routes.osc
```

## Python API Usage

You can also use the converter as a Python library in your own code:

```python
from gtfstoosm.convert import convert_gtfs_to_osm

# Basic conversion
convert_gtfs_to_osm('path/to/gtfs.zip', 'output.osc')

# Conversion with options
convert_gtfs_to_osm(
    'path/to/gtfs.zip',  # gtfs_path
    'output.osc',  # osm_path
    exclude_stops=False,
    exclude_routes=False,
    add_missing_stops=True,
    stop_search_radius=5.0,
    add_route_direction=True,
    route_ref_pattern='^[0-9]+$'
)
```

## Output Format

The converter generates standard OSM XML files containing:

1. Nodes for the transit stops that are not found in OSM (only with `--add-missing-stops`), with tags:
   - `public_transport=platform`
   - `highway=bus_stop`
   - `name=*` (from GTFS stop_name)

2. Relations for routes with tags:
   - `type=route`
   - `route=*` (bus, tram, train, etc. based on GTFS route_type)
   - `name=*` (built from GTFS route_short_name and route_long_name)
   - `ref=*` (from GTFS route_short_name)
   - `public_transport:version=2`

3. Route master relations for grouping variants of the same route

## Importing to OSM

The generated OSM files are not meant to be uploaded directly to OpenStreetMap. Note that this application does not check for existing relations. Instead:

1. Use the JOSM editor to open the generated OSM file
2. Review and adjust the data as needed
3. Upload using proper changeset tags and comments

Always follow the [OSM Import Guidelines](https://wiki.openstreetmap.org/wiki/Import/Guidelines) when importing any data to OpenStreetMap.

## Troubleshooting

### Common Issues

1. **Invalid GTFS feed**: Ensure your GTFS feed is valid by checking it with a GTFS validator
2. **Missing required files**: The GTFS feed must contain at least the required files (stops.txt, routes.txt, trips.txt, stop_times.txt)
3. **Memory issues with large feeds**: For very large GTFS feeds, convert fewer routes at once with `--route-ref-pattern`

### Logging

Use the `--verbose` flag to enable detailed logging for troubleshooting:

```bash
python -m gtfstoosm.cli -i feed.zip -o output.osm --verbose
```
