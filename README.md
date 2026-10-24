# GTFS to OSM Converter

Turns a public transport feed ([GTFS](https://gtfs.org/)) into a file of OpenStreetMap
[public transport v2](https://wiki.openstreetmap.org/wiki/Public_transport) objects
(route relations, route masters and, optionally, stops) that you review and upload
with [JOSM](https://josm.openstreetmap.de/).

It is a helper for mappers, not an automatic importer: it never uploads anything,
and every result has to be checked by a person before it goes into OpenStreetMap.

## What it does

For each route of the feed, and for each distinct sequence of stops used by its trips:

1. **Stops**: looks for an existing OSM bus stop (`highway=bus_stop` or
   `public_transport=platform`) within a few metres of each GTFS stop. With
   `--add-missing-stops`, stops that are not found are added as new nodes.
2. **Ways**: follows the trip's shape (`shapes.txt`) on the road network and lists the
   OSM ways it uses.
3. **Relations**: writes a `type=route` relation (stops as `platform` members, then
   ways) per stop sequence, and a `type=route_master` relation grouping them.

The result is an [osmChange](https://wiki.openstreetmap.org/wiki/OsmChange) file
(`.osc`) whose new objects have negative IDs.

What it does **not** do:

- It does not check whether the relations already exist in OSM: look before you add.
- It does not upload anything.
- It does not support feeds without `shapes.txt` yet (it stops with `KeyError: 'shapes'`).
  [pfaedle](#quick-start-with-docker-any-os) can generate the shapes.
- Bus routes are the supported case.

### Online services used

The lookups are made on public servers, so please convert one network at a time and
avoid repeated runs on large feeds. These addresses are written in `convert.py`.

| What | Service | What is sent |
|---|---|---|
| Existing OSM stops | [Overpass API](https://wiki.openstreetmap.org/wiki/Overpass_API) instance `maps.mail.ru` | Coordinates of the stops |
| Ways followed by a trip | [Valhalla](https://valhalla.github.io/valhalla/) instance `valhalla1.openstreetmap.de` | Points of the trip shape |

An internet connection is therefore required (except to run the tests).

## Quick start with Docker (any OS)

Docker is the easiest way to run the tool: nothing else to install, the same result
on Linux, macOS and Windows.

1. Install Docker (see [Prerequisites](#prerequisites)).
2. Get the code and create a folder for your files:

   ```bash
   git clone https://github.com/whubsch/gtfstoosm.git
   cd gtfstoosm
   mkdir data
   ```

3. Copy your GTFS feed (a `.zip`) into `data/`, then run:

   ```bash
   docker compose run --rm convert -i /data/feed.zip -o /data/routes.osc
   ```

   The first run builds the image, which takes a few minutes. `data/routes.osc` is
   the result.

4. Open `routes.osc` in JOSM (see [Reviewing and uploading](#reviewing-and-uploading)).

On Windows with **Git Bash**, put `MSYS_NO_PATHCONV=1` in front of the command,
otherwise `/data/...` is rewritten into a Windows path. PowerShell and cmd do not
need it.

Other commands:

```bash
docker compose run --rm convert --help                  # all options
docker compose --profile tools run --rm osmium --help   # osmium-tool, to cut OSM extracts
docker compose --profile tools run --rm pfaedle -x /data/region.osm.pbf /data/feed.zip
```

[`pfaedle`](https://github.com/ad-freiburg/pfaedle) (GPL-3.0, official image) generates
`shapes.txt` for feeds that lack them, from an OSM extract. It writes a copy of the feed
to `data/gtfs-out`.

## Prerequisites

| | With Docker (recommended) | Without Docker |
|---|---|---|
| **Linux** | [Docker Engine](https://docs.docker.com/engine/install/) with the Compose plugin (`docker compose version` must work) | Python 3.12, `python3-venv`, git |
| **macOS** | [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/) | Python 3.12 (for instance `brew install python@3.12`), git |
| **Windows** | [Docker Desktop](https://docs.docker.com/desktop/setup/install/windows-install/) with the WSL 2 backend, git | Python 3.12 from [python.org](https://www.python.org/downloads/), git |

In both cases you also need [JOSM](https://josm.openstreetmap.de/) to review the result
and an OpenStreetMap account to upload it.

## Installation without Docker

```bash
git clone https://github.com/whubsch/gtfstoosm.git
cd gtfstoosm
python -m venv .venv
```

Activate the environment:

- Linux and macOS: `source .venv/bin/activate`
- Windows (PowerShell): `.venv\Scripts\Activate.ps1`
- Windows (cmd): `.venv\Scripts\activate.bat`

Then install and run:

```bash
pip install .
python -m gtfstoosm.cli --input path/to/feed.zip --output routes.osc
```

If `pip` tries to compile the `regex` package and fails on Windows with
*"Microsoft Visual C++ 14.0 or greater is required"*, your Python version has no
prebuilt package yet: use Python 3.12, or Docker.

## Usage

```bash
# whole feed, with the stops missing from OSM added
python -m gtfstoosm.cli -i feed.zip -o routes.osc --add-missing-stops

# only the routes whose route_id is numeric, tagged with their network
python -m gtfstoosm.cli -i feed.zip -o routes.osc \
    --route-ref-pattern '^[0-9]+$' \
    --relation-tags 'network=My Bus;operator=My Agency'
```

The main options (all of them are in [docs/usage.md](docs/usage.md)):

| Option | Effect |
|---|---|
| `-i`, `--input` | GTFS feed (`.zip`) |
| `-o`, `--output` | Output `.osc` file |
| `--add-missing-stops` | Add stops that are not in OSM as new nodes |
| `--exclude-stops`, `--exclude-routes` | Leave the stops, or the routes, out of the output |
| `--route-ref-pattern REGEX` | Only convert routes whose `route_id` matches |
| `--relation-tags 'k=v;k=v'` | Tags added to the route and route_master relations |
| `--stop-search-radius M` | Distance in metres to match a GTFS stop to an OSM stop (10 at most) |
| `--add-route-direction` | Add the compass direction to route names |
| `-v`, `--verbose` | Detailed log |

## Reviewing and uploading

1. Open JOSM and download the area of the network (*File → Download data*), so that the
   stops and ways the relations refer to are loaded.
2. Open the `.osc` file (*File → Open*).
3. Check each relation: stop order, ways, gaps in the line, the `name`, `ref` and
   `network` tags, and lines that already exist.
4. Upload with a meaningful changeset comment.

Before importing a feed, make sure that:

- its **licence is compatible** with OpenStreetMap's (ODbL), or that the publisher
  allowed the import. Many feeds, in particular in France, are published under the
  *Licence Ouverte*: its compatibility is discussed on the
  [OSM legal-talk list](https://lists.openstreetmap.org/pipermail/legal-talk/2020-September/008924.html),
  so check the current position of the Licensing Working Group first;
- you followed the [Import Guidelines](https://wiki.openstreetmap.org/wiki/Import/Guidelines)
  and discussed the import with the local community.

## Output

- **Stops** (only with `--add-missing-stops`): `public_transport=platform`,
  `highway=bus_stop`, `name`.
- **Routes**: `type=route`, `public_transport:version=2`, `route` (from the GTFS
  `route_type`), `ref` (from `route_short_name`), `name`, and `colour` when the route has
  one.
- **Route masters**: `type=route_master`, grouping the routes that have the same `ref`.

## Troubleshooting

- *Conversion fails*: run with `-v`, and check the feed with a
  [GTFS validator](https://gtfs.org/resources/validation/).
- *`KeyError: 'shapes'`*: the feed has no `shapes.txt`. Generate it with `pfaedle`.
- *"Request error by Overpass API (HTTP 504)"* or *429*: the public server is busy. The
  tool retries a few times; try again later, and use `--route-ref-pattern` to convert
  fewer routes at once.

## Development

Everything runs in Docker. `Dockerfile.dev` builds an image without the source: compose
mounts the `gtfstoosm/` and `tests/` folders of your disk in it, so a change is seen at
once, with no rebuild. `up` starts it, and the tests run again at each change:

```bash
docker compose --profile dev up                                          # the tests, again at each change
docker compose exec dev gtfstoosm -i /data/feed.zip -o /data/out.osc     # in another terminal: the tool, from your code
docker compose exec dev pytest                                           # the tests, once
docker compose exec dev ruff check .                                     # lint
```

(Without `up -d`, the same work in a one-shot container:
`docker compose --profile dev run --rm dev pytest`.) To bring the image up to date,
`docker compose --profile dev build --pull dev`. The image used for running the tool
(`Dockerfile`, service `convert`) has the code inside: after a change, rebuild it with
`docker compose build --pull convert`.

Without Docker, with [uv](https://docs.astral.sh/uv/):

```bash
uv run --python 3.12 --with pytest python -m pytest -m "not slow"
```

Tests marked `slow` call the online services and are skipped here. `nox` runs the tests
on several Python versions (`nox -s tests`), and has `lint` and `type_check` sessions.

```
gtfstoosm/
├── gtfstoosm/       # package: cli.py, convert.py, gtfs.py, osm.py, utils.py
├── tests/
├── docs/usage.md    # all the options, and the Python API
├── Dockerfile       # the tool
├── Dockerfile.dev   # for working on the code
├── docker-compose.yml
└── pyproject.toml
```

Contributions are welcome: fork, create a branch, and open a pull request.

## License

MIT, see [LICENSE](LICENSE).

## Acknowledgments

- [GTFS Reference](https://gtfs.org/documentation/schedule/reference/)
- [OpenStreetMap Wiki: Public transport](https://wiki.openstreetmap.org/wiki/Public_transport)
