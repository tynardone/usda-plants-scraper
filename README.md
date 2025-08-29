# USDA PLANTS Data Fetcher

This script downloads plant profile and characterisitcs data from the USDA PLANTS Database,
normalizes it and exports as CSV files for analysis in  `pandas` or other tools

## Features

- Asynchronous fetching using [`httpx`](https://www.python-httpx.org/)
- Concurrency controls with `asyncio.Semaphore` and exponential backoff with jitter
- Data normalization into four tables related through 'PlantId'
  - `plants.csv` - one row per plant
  - `native_statuses.csv` - zero or more native status rows per plant
  - `ancestors.csv` - zero or more ancestor relationships per plant
  - `characteristics.csv` - zero or more plant characterisitc rows per plant
- Output as CSV files ready for further analysis

## Requirements

- Python 3.11+
- Dependencis in [`requirements.txt`](#requirements-txt)

## Main libraries used

- `httpx` - async HTTP client
- `pandas` - dataframes and CSV export
- `tqdm` - progress bar

## Installation

Clone this repository and install dependencies:

```bash
```bash
git clone https://github.com/yourusername/usda-plants-fetcher.git
cd usda-plants-fetcher
python -m venv .venv
source .venv/bin/activate   # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

1. Create an `input.csv` file that contains at least a 'Symbol' column with USDA plant symbols to fetch:
    - These can be easily filtered and downloaded from [USDA PLANTS Database](https://plants.usda.gov/)

2. Run the script

    ```bash
    python plants_fetcher.py
    ```

3. The script will:
    - Fetch data for each symbol in `input.csv`
    - Normalize results into rows
    - Write CSV files into `data/` folder

## Project Structure

```bash
plants_fetcher.py     # main script
input.csv             # list of plant symbols (user-provided)
data/
  ├─ plants.csv
  ├─ native_status.csv
  ├─ ancestors.csv
  └─ characteristics.csv
```

## Notes

## TODO

- Create attrs models for data rows

- Split apart sync and async code

## License

MIT License see [LICENSE](LICENSE) for details.
