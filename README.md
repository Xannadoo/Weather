# Weather Data CSV

Simple script to collect daily weather data for your location and store it in a CSV for analysis.

## What it does

- Stores one row per day
- Supports one-time historical backfill
- Supports periodic updates that append only new dates (no duplicates)
- Stores observed and forecast data in separate files by default
- Handles OpenWeather free-plan limits by using available fallback sources when needed

## Setup

1. Create and activate your virtual environment (optional but recommended).
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Add your values to `.env`:

```env
APIKEY=your_openweather_api_key
LAT=your_latitude
LON=your_longitude
```

## Usage

### 1) Backfill a date range

```bash
python app.py --mode backfill --start-date 2025-01-01 --end-date 2025-12-31
```

Default output: `weather_actuals.csv`

### 2) Append new daily data

```bash
python app.py --mode update
```

Default output: `weather_predictions.csv`

### 3) Compare predictions against historic trends

```bash
python trend_analysis.py --actuals-csv weather_actuals.csv --predictions-csv weather_predictions.csv
```

This runs a separate analysis step that compares each forecast row to the recent historic pattern and a wider rolling baseline, then prints a reliability estimate for each predicted day.

Optional output file:

```bash
python trend_analysis.py --output-csv weather_trend_report.csv
```

## Output

- Files:
	- `weather_actuals.csv` for observed/historical data
	- `weather_predictions.csv` for forecast data
	- `weather_trend_report.csv` for reliability analysis if you export it
- One row per day per file
- Re-running commands will skip dates already in the target CSV

## Helpful options

```bash
python app.py --help
```

Common options:
- `--csv-path` single-file override (optional)
- `--actuals-csv` custom observed file path
- `--predictions-csv` custom forecast file path
- `--units` one of `metric`, `imperial`, `standard`
- `--max-days` safety cap for backfill size
