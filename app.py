
import argparse
import csv
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set

import requests


ONECALL_URL = "https://api.openweathermap.org/data/3.0/onecall"
DAY_SUMMARY_URL = "https://api.openweathermap.org/data/3.0/onecall/day_summary"
FORECAST_25_URL = "https://api.openweathermap.org/data/2.5/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

# Stable schema shared by backfill and update modes.
CSV_COLUMNS = [
	"date",
	"source",
	"lat",
	"lon",
	"timezone",
	"units",
	"temp_morning",
	"temp_day",
	"temp_evening",
	"temp_night",
	"temp_min",
	"temp_max",
	"feels_like_morning",
	"feels_like_day",
	"feels_like_evening",
	"feels_like_night",
	"humidity",
	"pressure",
	"clouds",
	"wind_speed",
	"wind_deg",
	"wind_gust",
	"pop",
	"rain",
	"snow",
	"uvi",
	"weather_main",
	"weather_description",
]


def load_dotenv(dotenv_path: Path = Path(".env")) -> None:
	if not dotenv_path.exists():
		return

	for raw_line in dotenv_path.read_text(encoding="utf-8").splitlines():
		line = raw_line.strip()
		if not line or line.startswith("#") or "=" not in line:
			continue

		key, value = line.split("=", 1)
		key = key.strip()
		value = value.strip().strip("\"").strip("'")
		os.environ.setdefault(key, value)


def first_env(*names: str) -> Optional[str]:
	for name in names:
		value = os.getenv(name)
		if value:
			return value
	return None


def parse_date(value: str) -> date:
	try:
		return datetime.strptime(value, "%Y-%m-%d").date()
	except ValueError as exc:
		raise ValueError(f"Invalid date '{value}'. Use YYYY-MM-DD.") from exc


def date_range(start: date, end: date) -> Iterable[date]:
	current = start
	while current <= end:
		yield current
		current += timedelta(days=1)


def get_existing_dates(csv_path: Path) -> Set[str]:
	if not csv_path.exists():
		return set()

	existing = set()
	with csv_path.open("r", newline="", encoding="utf-8") as handle:
		reader = csv.DictReader(handle)
		for row in reader:
			day = row.get("date")
			if day:
				existing.add(day)
	return existing


def write_rows(csv_path: Path, rows: List[Dict[str, Any]]) -> None:
	if not rows:
		return

	csv_path.parent.mkdir(parents=True, exist_ok=True)
	file_exists = csv_path.exists()

	with csv_path.open("a", newline="", encoding="utf-8") as handle:
		writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS)
		if not file_exists:
			writer.writeheader()
		for row in rows:
			writer.writerow({column: row.get(column, "") for column in CSV_COLUMNS})


def fetch_day_summary(
	session: requests.Session,
	*,
	api_key: str,
	lat: float,
	lon: float,
	day: date,
	units: str,
	timeout_seconds: int,
) -> Dict[str, Any]:
	params = {
		"lat": lat,
		"lon": lon,
		"date": day.isoformat(),
		"units": units,
		"appid": api_key,
	}
	response = session.get(DAY_SUMMARY_URL, params=params, timeout=timeout_seconds)
	response.raise_for_status()
	return response.json()


def fetch_daily_forecast(
	session: requests.Session,
	*,
	api_key: str,
	lat: float,
	lon: float,
	units: str,
	timeout_seconds: int,
) -> Dict[str, Any]:
	params = {
		"lat": lat,
		"lon": lon,
		"units": units,
		"exclude": "current,minutely,hourly,alerts",
		"appid": api_key,
	}
	response = session.get(ONECALL_URL, params=params, timeout=timeout_seconds)
	response.raise_for_status()
	return response.json()


def fetch_forecast_25(
	session: requests.Session,
	*,
	api_key: str,
	lat: float,
	lon: float,
	units: str,
	timeout_seconds: int,
) -> Dict[str, Any]:
	params = {
		"lat": lat,
		"lon": lon,
		"units": units,
		"appid": api_key,
	}
	response = session.get(FORECAST_25_URL, params=params, timeout=timeout_seconds)
	response.raise_for_status()
	return response.json()


def api_error_message(exc: requests.exceptions.HTTPError) -> str:
	response = exc.response
	if response is None:
		return str(exc)

	try:
		payload = response.json()
		if isinstance(payload, dict) and payload.get("message"):
			return str(payload["message"])
	except ValueError:
		pass

	return response.text[:300].strip() or str(exc)


def is_onecall_subscription_error(exc: requests.exceptions.HTTPError) -> bool:
	response = exc.response
	if response is None:
		return False
	if response.status_code != 401:
		return False
	message = api_error_message(exc).lower()
	return "one call 3.0 requires a separate subscription" in message


def flatten_day_summary(payload: Dict[str, Any], lat: float, lon: float, units: str) -> Dict[str, Any]:
	temperature = payload.get("temperature", {})
	wind_max = payload.get("wind", {}).get("max", {})

	return {
		"date": payload.get("date", ""),
		"source": "onecall_day_summary",
		"lat": payload.get("lat", lat),
		"lon": payload.get("lon", lon),
		"timezone": payload.get("tz", ""),
		"units": payload.get("units", units),
		"temp_morning": temperature.get("morning", ""),
		"temp_day": temperature.get("afternoon", ""),
		"temp_evening": temperature.get("evening", ""),
		"temp_night": temperature.get("night", ""),
		"temp_min": temperature.get("min", ""),
		"temp_max": temperature.get("max", ""),
		"feels_like_morning": "",
		"feels_like_day": "",
		"feels_like_evening": "",
		"feels_like_night": "",
		"humidity": payload.get("humidity", {}).get("afternoon", ""),
		"pressure": payload.get("pressure", {}).get("afternoon", ""),
		"clouds": payload.get("cloud_cover", {}).get("afternoon", ""),
		"wind_speed": wind_max.get("speed", ""),
		"wind_deg": wind_max.get("direction", ""),
		"wind_gust": "",
		"pop": "",
		"rain": payload.get("precipitation", {}).get("total", ""),
		"snow": "",
		"uvi": "",
		"weather_main": "",
		"weather_description": "",
	}


def flatten_onecall_daily(
	payload: Dict[str, Any],
	daily_item: Dict[str, Any],
	units: str,
) -> Dict[str, Any]:
	daily_date = datetime.utcfromtimestamp(int(daily_item["dt"]))
	weather = daily_item.get("weather", [{}])
	primary_weather = weather[0] if weather else {}

	temp = daily_item.get("temp", {})
	feels = daily_item.get("feels_like", {})

	return {
		"date": daily_date.date().isoformat(),
		"source": "onecall_daily_forecast",
		"lat": payload.get("lat", ""),
		"lon": payload.get("lon", ""),
		"timezone": payload.get("timezone", ""),
		"units": units,
		"temp_morning": temp.get("morn", ""),
		"temp_day": temp.get("day", ""),
		"temp_evening": temp.get("eve", ""),
		"temp_night": temp.get("night", ""),
		"temp_min": temp.get("min", ""),
		"temp_max": temp.get("max", ""),
		"feels_like_morning": feels.get("morn", ""),
		"feels_like_day": feels.get("day", ""),
		"feels_like_evening": feels.get("eve", ""),
		"feels_like_night": feels.get("night", ""),
		"humidity": daily_item.get("humidity", ""),
		"pressure": daily_item.get("pressure", ""),
		"clouds": daily_item.get("clouds", ""),
		"wind_speed": daily_item.get("wind_speed", ""),
		"wind_deg": daily_item.get("wind_deg", ""),
		"wind_gust": daily_item.get("wind_gust", ""),
		"pop": daily_item.get("pop", ""),
		"rain": daily_item.get("rain", ""),
		"snow": daily_item.get("snow", ""),
		"uvi": daily_item.get("uvi", ""),
		"weather_main": primary_weather.get("main", ""),
		"weather_description": primary_weather.get("description", ""),
	}


def rows_from_forecast_25(payload: Dict[str, Any], units: str) -> List[Dict[str, Any]]:
	timezone_offset = int(payload.get("city", {}).get("timezone", 0))
	city = payload.get("city", {})
	lat = city.get("coord", {}).get("lat", "")
	lon = city.get("coord", {}).get("lon", "")

	best_by_day: Dict[str, Dict[str, Any]] = {}

	for item in payload.get("list", []):
		dt_utc = datetime.utcfromtimestamp(int(item["dt"]))
		dt_local = dt_utc + timedelta(seconds=timezone_offset)
		local_date = dt_local.date().isoformat()

		score = abs(dt_local.hour - 12)
		existing = best_by_day.get(local_date)
		if existing and score >= existing["_score"]:
			continue

		best_by_day[local_date] = {"_score": score, "item": item, "local_date": local_date}

	rows: List[Dict[str, Any]] = []
	for local_date in sorted(best_by_day.keys()):
		data = best_by_day[local_date]["item"]
		main = data.get("main", {})
		weather = data.get("weather", [{}])
		wind = data.get("wind", {})
		clouds = data.get("clouds", {})
		rain = data.get("rain", {})
		snow = data.get("snow", {})
		primary_weather = weather[0] if weather else {}

		rows.append(
			{
				"date": local_date,
				"source": "openweather_2_5_forecast",
				"lat": lat,
				"lon": lon,
				"timezone": timezone_offset,
				"units": units,
				"temp_morning": "",
				"temp_day": main.get("temp", ""),
				"temp_evening": "",
				"temp_night": "",
				"temp_min": main.get("temp_min", ""),
				"temp_max": main.get("temp_max", ""),
				"feels_like_morning": "",
				"feels_like_day": main.get("feels_like", ""),
				"feels_like_evening": "",
				"feels_like_night": "",
				"humidity": main.get("humidity", ""),
				"pressure": main.get("pressure", ""),
				"clouds": clouds.get("all", ""),
				"wind_speed": wind.get("speed", ""),
				"wind_deg": wind.get("deg", ""),
				"wind_gust": wind.get("gust", ""),
				"pop": data.get("pop", ""),
				"rain": rain.get("3h", ""),
				"snow": snow.get("3h", ""),
				"uvi": "",
				"weather_main": primary_weather.get("main", ""),
				"weather_description": primary_weather.get("description", ""),
			}
		)

	return rows


def weather_code_to_text(code: Any) -> str:
	mapping = {
		0: "clear sky",
		1: "mainly clear",
		2: "partly cloudy",
		3: "overcast",
		45: "fog",
		48: "depositing rime fog",
		51: "light drizzle",
		53: "moderate drizzle",
		55: "dense drizzle",
		56: "light freezing drizzle",
		57: "dense freezing drizzle",
		61: "slight rain",
		63: "moderate rain",
		65: "heavy rain",
		66: "light freezing rain",
		67: "heavy freezing rain",
		71: "slight snow fall",
		73: "moderate snow fall",
		75: "heavy snow fall",
		77: "snow grains",
		80: "slight rain showers",
		81: "moderate rain showers",
		82: "violent rain showers",
		85: "slight snow showers",
		86: "heavy snow showers",
		95: "thunderstorm",
		96: "thunderstorm with slight hail",
		99: "thunderstorm with heavy hail",
	}
	return mapping.get(int(code), "unknown") if str(code).strip() else ""


def weather_code_group(code: Any) -> str:
	if str(code).strip() == "":
		return ""
	value = int(code)
	if value == 0:
		return "Clear"
	if value in (1, 2, 3, 45, 48):
		return "Clouds"
	if value in (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82):
		return "Rain"
	if value in (71, 73, 75, 77, 85, 86):
		return "Snow"
	if value in (95, 96, 99):
		return "Thunderstorm"
	return "Weather"


def fetch_open_meteo_archive(
	session: requests.Session,
	*,
	lat: float,
	lon: float,
	start: date,
	end: date,
	timeout_seconds: int,
) -> Dict[str, Any]:
	params = {
		"latitude": lat,
		"longitude": lon,
		"start_date": start.isoformat(),
		"end_date": end.isoformat(),
		"timezone": "auto",
		"daily": ",".join(
			[
				"temperature_2m_max",
				"temperature_2m_min",
				"temperature_2m_mean",
				"apparent_temperature_max",
				"apparent_temperature_min",
				"precipitation_sum",
				"rain_sum",
				"snowfall_sum",
				"wind_speed_10m_max",
				"wind_gusts_10m_max",
				"wind_direction_10m_dominant",
				"relative_humidity_2m_mean",
				"cloud_cover_mean",
				"weather_code",
			]
		),
	}
	response = session.get(OPEN_METEO_ARCHIVE_URL, params=params, timeout=timeout_seconds)
	response.raise_for_status()
	return response.json()


def rows_from_open_meteo_archive(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
	daily = payload.get("daily", {})
	dates = daily.get("time", [])
	rows: List[Dict[str, Any]] = []

	for idx, day in enumerate(dates):
		code = daily.get("weather_code", [""])[idx]
		rain_sum = daily.get("rain_sum", [""])[idx]
		precipitation_sum = daily.get("precipitation_sum", [""])[idx]
		rows.append(
			{
				"date": day,
				"source": "open_meteo_archive",
				"lat": payload.get("latitude", ""),
				"lon": payload.get("longitude", ""),
				"timezone": payload.get("timezone", ""),
				"units": "metric",
				"temp_morning": "",
				"temp_day": daily.get("temperature_2m_mean", [""])[idx],
				"temp_evening": "",
				"temp_night": "",
				"temp_min": daily.get("temperature_2m_min", [""])[idx],
				"temp_max": daily.get("temperature_2m_max", [""])[idx],
				"feels_like_morning": "",
				"feels_like_day": daily.get("apparent_temperature_max", [""])[idx],
				"feels_like_evening": "",
				"feels_like_night": "",
				"humidity": daily.get("relative_humidity_2m_mean", [""])[idx],
				"pressure": "",
				"clouds": daily.get("cloud_cover_mean", [""])[idx],
				"wind_speed": daily.get("wind_speed_10m_max", [""])[idx],
				"wind_deg": daily.get("wind_direction_10m_dominant", [""])[idx],
				"wind_gust": daily.get("wind_gusts_10m_max", [""])[idx],
				"pop": "",
				"rain": rain_sum if rain_sum != "" else precipitation_sum,
				"snow": daily.get("snowfall_sum", [""])[idx],
				"uvi": "",
				"weather_main": weather_code_group(code),
				"weather_description": weather_code_to_text(code),
			}
		)

	return rows


def build_arg_parser() -> argparse.ArgumentParser:
	parser = argparse.ArgumentParser(description="Backfill and append daily weather data to CSV.")
	parser.add_argument(
		"--mode",
		choices=["backfill", "update"],
		required=True,
		help="backfill: date range export; update: append new daily rows.",
	)
	parser.add_argument(
		"--csv-path",
		default=None,
		help="Optional single CSV path override for either mode.",
	)
	parser.add_argument(
		"--actuals-csv",
		default="weather_actuals.csv",
		help="CSV path for observed/historical rows (default: weather_actuals.csv)",
	)
	parser.add_argument(
		"--predictions-csv",
		default="weather_predictions.csv",
		help="CSV path for forecast rows (default: weather_predictions.csv)",
	)
	parser.add_argument(
		"--units",
		choices=["standard", "metric", "imperial"],
		default="metric",
		help="Units for API requests (default: metric).",
	)
	parser.add_argument("--start-date", help="Backfill start date in YYYY-MM-DD.")
	parser.add_argument("--end-date", help="Backfill end date in YYYY-MM-DD (defaults to start date).")
	parser.add_argument(
		"--max-days",
		type=int,
		default=366,
		help="Safety cap for backfill date range size (default: 366).",
	)
	parser.add_argument(
		"--timeout",
		type=int,
		default=30,
		help="HTTP timeout in seconds (default: 30).",
	)
	return parser


def main() -> None:
	parser = build_arg_parser()
	args = parser.parse_args()

	load_dotenv()

	api_key = first_env("OPENWEATHER_API_KEY", "APIKEY", "OWM_API_KEY")
	lat_raw = first_env("LAT", "LATITUDE")
	lon_raw = first_env("LON", "LONGITUDE")

	missing = []
	if not api_key:
		missing.append("OPENWEATHER_API_KEY")
	if not lat_raw:
		missing.append("LAT")
	if not lon_raw:
		missing.append("LON")
	if missing:
		parser.error(
			"Missing required environment variable(s): "
			+ ", ".join(missing)
			+ ". Add them to .env."
		)

	assert api_key is not None and lat_raw is not None and lon_raw is not None

	try:
		lat = float(lat_raw)
		lon = float(lon_raw)
	except ValueError as exc:
		parser.error(f"LAT and LON must be numeric. Details: {exc}")

	if args.csv_path:
		csv_path = Path(args.csv_path)
	elif args.mode == "backfill":
		csv_path = Path(args.actuals_csv)
	else:
		csv_path = Path(args.predictions_csv)

	existing_dates = get_existing_dates(csv_path)
	new_rows: List[Dict[str, Any]] = []
	api_calls = 0

	session = requests.Session()

	if args.mode == "backfill":
		if not args.start_date:
			parser.error("--start-date is required for --mode backfill")

		start = parse_date(args.start_date)
		end = parse_date(args.end_date) if args.end_date else start

		if end < start:
			parser.error("--end-date cannot be before --start-date")

		all_days = [d for d in date_range(start, end)]
		if len(all_days) > args.max_days:
			parser.error(
				f"Requested {len(all_days)} days, above --max-days={args.max_days}. "
				"Use a smaller range or increase --max-days deliberately."
			)

		days_to_fetch = [d for d in all_days if d.isoformat() not in existing_dates]

		used_fallback = False
		try:
			for day in days_to_fetch:
				payload = fetch_day_summary(
					session,
					api_key=api_key,
					lat=lat,
					lon=lon,
					day=day,
					units=args.units,
					timeout_seconds=args.timeout,
				)
				api_calls += 1
				row = flatten_day_summary(payload, lat=lat, lon=lon, units=args.units)
				if row.get("date") and row["date"] not in existing_dates:
					new_rows.append(row)
					existing_dates.add(row["date"])
		except requests.exceptions.HTTPError as exc:
			if not is_onecall_subscription_error(exc):
				parser.error(f"OpenWeather request failed: {api_error_message(exc)}")
			used_fallback = True

		if used_fallback:
			forecast_payload = fetch_forecast_25(
				session,
				api_key=api_key,
				lat=lat,
				lon=lon,
				units=args.units,
				timeout_seconds=args.timeout,
			)
			api_calls += 1

			by_day_rows = rows_from_forecast_25(forecast_payload, units=args.units)
			for row in by_day_rows:
				row_date = parse_date(row["date"])
				if start <= row_date <= end and row["date"] not in existing_dates:
					new_rows.append(row)
					existing_dates.add(row["date"])

			missing_requested = [d for d in all_days if d.isoformat() not in existing_dates]
			if missing_requested:
				archive_payload = fetch_open_meteo_archive(
					session,
					lat=lat,
					lon=lon,
					start=missing_requested[0],
					end=missing_requested[-1],
					timeout_seconds=args.timeout,
				)
				api_calls += 1

				archive_rows = rows_from_open_meteo_archive(archive_payload)
				missing_set = {d.isoformat() for d in missing_requested}
				for row in archive_rows:
					row_date = row.get("date")
					if row_date and row_date in missing_set and row_date not in existing_dates:
						new_rows.append(row)
						existing_dates.add(row_date)

			if not new_rows:
				parser.error(
					"Backfill returned no rows. Check date range and API availability for your location."
				)

	elif args.mode == "update":
		try:
			payload = fetch_daily_forecast(
				session,
				api_key=api_key,
				lat=lat,
				lon=lon,
				units=args.units,
				timeout_seconds=args.timeout,
			)
			api_calls += 1

			for item in payload.get("daily", []):
				row = flatten_onecall_daily(payload, item, units=args.units)
				row_date = row.get("date")
				if row_date and row_date not in existing_dates:
					new_rows.append(row)
					existing_dates.add(row_date)
		except requests.exceptions.HTTPError as exc:
			if not is_onecall_subscription_error(exc):
				parser.error(f"OpenWeather request failed: {api_error_message(exc)}")

			forecast_payload = fetch_forecast_25(
				session,
				api_key=api_key,
				lat=lat,
				lon=lon,
				units=args.units,
				timeout_seconds=args.timeout,
			)
			api_calls += 1

			for row in rows_from_forecast_25(forecast_payload, units=args.units):
				row_date = row.get("date")
				if row_date and row_date not in existing_dates:
					new_rows.append(row)
					existing_dates.add(row_date)

	write_rows(csv_path, new_rows)

	print(f"Mode: {args.mode}")
	print(f"CSV: {csv_path.resolve()}")
	print(f"API calls made: {api_calls}")
	print(f"Rows appended: {len(new_rows)}")


if __name__ == "__main__":
	main()