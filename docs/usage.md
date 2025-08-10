# GTFS to OSM Converter Usage Guide

This guide explains how to use the GTFS to OSM converter to transform public transit data from the General Transit Feed Specification (GTFS) format into OpenStreetMap (OSM) relations.

## Basic Usage

The converter can be used either as a command-line tool or as a Python library.

### Command-Line Interface

The basic syntax for the command-line interface is:

```bash
python -m gtfstoosm.cli --input /path/to/gtfs.zip --output output.osc
```

### Required Arguments

- `--input`, `-i`: Path to the input GTFS feed zip file
- `--output`, `-o`: Path where the output OSM XML file will be written

### Optional Arguments

- `--exclude-stops`: Exclude stops from the output
- `--exclude-routes`: Exclude routes from the output
- `--add-missing-stops`: Add missing stops to the output
- `--route-types`: Only include routes with specific GTFS route_type values (space-separated)
- `--agency`: Only include routes for a specific agency ID
- `--verbose`, `-v`: Enable verbose logging for debugging

## Examples

### Basic Conversion

Convert an entire GTFS feed to OSM:

```bash
python -m gtfstoosm.cli -i transit_agency.zip -o transit_routes.osc
```

### Converting Only Specific Route Types

Convert only tram (0), subway (1), and rail (2) routes:

```bash
python -m gtfstoosm.cli -i transit_agency.zip -o transit_rail.osm --route-types 0 1 2
```

### Converting Only Routes for a Specific Agency

If your GTFS feed contains data from multiple agencies:

```bash
python -m gtfstoosm.cli -i regional_transit.zip -o city_bus.osm --agency CITYBUS
```

## Python API Usage

You can also use the converter as a Python library in your own code:

```python
from gtfstoosm.convert import convert_gtfs_to_osm

# Basic conversion
convert_gtfs_to_osm('path/to/gtfs.zip', 'output.osm')

# Conversion with options
options = {
    'include_stops': True,
    'include_routes': True,
    'route_types': [0, 1, 2],  # Only tram, subway, rail
    'agency_id': 'AGENCY1'
}
convert_gtfs_to_osm('path/to/gtfs.zip', 'output.osm', **options)
```

## Output Format

The converter generates standard OSM XML files containing:

1. Nodes for transit stops with appropriate tags:
   - `public_transport=platform`
   - `highway=bus_stop`
   - `name=*` (from GTFS stop_name)

2. Relations for routes with tags:
   - `type=route`
   - `route=*` (bus, tram, train, etc. based on GTFS route_type)
   - `name=*` (from GTFS route_long_name)
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
3. **Memory issues with large feeds**: For very large GTFS feeds, try filtering by agency or route type

### Logging

Use the `--verbose` flag to enable detailed logging for troubleshooting:

```bash
python -m gtfstoosm.cli -i feed.zip -o output.osm --verbose
```
