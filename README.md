# GTFS to OSM Converter

A set of Python scripts to convert GTFS (General Transit Feed Specification) transit feeds to OpenStreetMap relations that can be imported manually.

## Overview

This project provides tools to transform public transit data from the GTFS format into OpenStreetMap PTv2 relations. These relations can then be imported into OSM to enhance public transportation mapping. Currently, only bus routes are supported.

## Features

- Convert GTFS routes to OSM relations
- Handle stops, routes, and schedule information
- Preserve transit metadata in OSM tags
- Validate conversion output for OSM compatibility

## Requirements

- Python 3.10+
- Required Python packages (see `requirements.txt`)
- Internet connection (for API access)

## Installation

1. Clone this repository:

   ```bash
   git clone https://github.com/yourusername/gtfstoosm.git
   cd gtfstoosm
   ```

2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Usage

Basic usage example:

```bash
python -m gtfstoosm.cli --input path/to/gtfs/feed.zip --output output.osc
```

For more detailed instructions and options, see the [Documentation](docs/usage.md).

## Project Structure

```
gtfstoosm/
├── gtfstoosm/          # Main package
│   ├── __init__.py
│   ├── convert.py      # Core conversion logic
│   ├── gtfs.py         # GTFS data handling
│   ├── osm.py          # OSM output generation
│   ├── utils.py        # Utility functions
│   └── cli.py          # Command-line interface
├── docs/               # Documentation
├── requirements.txt    # Python dependencies
└── LICENSE             # MIT License
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- [GTFS Reference](https://developers.google.com/transit/gtfs/reference)
- [OpenStreetMap Wiki](https://wiki.openstreetmap.org/wiki/Public_transport)
