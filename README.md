# MTAtabase

A Python library for parsing realtime MTA subway data and matching it against static GTFS schedules with no API key required.

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![Python 3.13+](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://img.shields.io/pypi/v/MTAtabase.svg)](https://pypi.org/project/MTAtabase/)

## Overview

Most MTA data tools require you to register for an API key and hit live endpoints to get realtime train information. MTAtabase parses an archive of MTA data feeds and correlates them with static GTFS schedule files locally, with no API credentials needed.

Third-party realtime data is sourced from [subwaydata.nyc](https://subwaydata.nyc).

## Features

- Parse MTA realtime train data without an API key
- Match live trip updates to static GTFS schedule data
- Works offline once data is fetched
- Built on pandas for easy data manipulation
- Includes a bundled schema for GTFS data structures

## Requirements

- Python 3.13+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

## Installation

### With pip

```bash
pip install MTAtabase
```

### From source

```bash
git clone https://github.com/pacmanboss256/mtatabase.git
cd mtatabase
uv sync
```

## Usage

```python
from MTAtabase import MTAtabase

# Initialize the database
db = MTAtabase(subway_data_path,gtfs_path)
```

## Project Structure

```
mtatabase/
├── mtatabase/      # Core library source
├── data/           # subwaydata.nyc data
├── gtfs/			# GTFS files
├── schema/         # Project schema definitions
└── pyproject.toml
```

## Data Sources

- **Realtime data:** [subwaydata.nyc](https://subwaydata.nyc) — a third-party archive of MTA subway feeds
- **Static GTFS:** MTA's published schedule files (stops, routes, trips, etc.)

## License

This project is licensed under the [GNU General Public License v3.0](LICENSE).

---

*This project is not affiliated with or endorsed by the Metropolitan Transportation Authority (MTA).*