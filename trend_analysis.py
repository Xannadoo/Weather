"""Forecast reliability analysis based on historic weather patterns.

This script is intentionally separate from data collection. It reads:
1) observed historic daily rows (actuals), and
2) forecast rows (predictions),
then estimates whether each forecast looks consistent with known patterns.

Reliability is built from three signals:
- Numeric consistency: are predicted values close to recent and seasonal baselines?
- Short-term trend alignment: does the next predicted temperature direction match
	the latest observed direction?
- Weather-pattern alignment: is the forecast weather category common in recent
	history for this period?

Final score is a weighted blend in the range 0-100.
"""

import argparse
import csv
from heapq import nsmallest
from collections import Counter
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Subset of fields used in reliability scoring.
RELIABILITY_NUMERIC_FIELDS = [
	"temp_day",
	"temp_min",
	"temp_max",
	"feels_like_day",
	"humidity",
	"wind_speed",
	"clouds",
	"pop",
	"rain",
]


@dataclass(frozen=True)
class Assessment:
	"""Single-day reliability result for one forecast row."""

	prediction_date: str
	reliability_score: int
	reliability_band: str
	trend_alignment: float
	seasonal_alignment: float
	notes: str


@dataclass(frozen=True)
class PatternMatch:
	"""Closest historic match candidate for a forecast day."""

	matched_date: date
	distance: float
	day_gap: int
	fields_used: int


def parse_date(value: str) -> date:
	"""Parse CSV date text using YYYY-MM-DD."""

	return datetime.strptime(value, "%Y-%m-%d").date()


def to_float(value: Any) -> Optional[float]:
	"""Convert value to float when possible; return None for blanks/invalid values."""

	if value is None:
		return None
	text = str(value).strip()
	if not text:
		return None
	try:
		return float(text)
	except ValueError:
		return None


def read_rows(csv_path: Path) -> List[Dict[str, Any]]:
	"""Read CSV rows as dictionaries."""

	with csv_path.open("r", newline="", encoding="utf-8") as handle:
		return list(csv.DictReader(handle))


def load_history(actuals_csv: Path) -> Dict[date, Dict[str, Any]]:
	"""Load historic rows keyed by date.

	Kept as a convenience helper for callers that want random date lookup.
	The main scoring flow uses sorted rows from `load_history_rows`.
	"""

	rows = read_rows(actuals_csv)
	history: Dict[date, Dict[str, Any]] = {}
	for row in rows:
		day_text = row.get("date", "").strip()
		if not day_text:
			continue
		try:
			day = parse_date(day_text)
		except ValueError:
			continue
		history[day] = row
	return history


def load_history_rows(actuals_csv: Path) -> List[Tuple[date, Dict[str, Any]]]:
	"""Load historic rows as a date-sorted sequence used by scoring functions."""

	rows = read_rows(actuals_csv)
	history_rows: List[Tuple[date, Dict[str, Any]]] = []
	for row in rows:
		day_text = row.get("date", "").strip()
		if not day_text:
			continue
		try:
			day = parse_date(day_text)
		except ValueError:
			continue
		history_rows.append((day, row))
	history_rows.sort(key=lambda item: item[0])
	return history_rows


def load_predictions(predictions_csv: Path) -> List[Dict[str, Any]]:
	"""Load prediction rows while keeping parsed date next to original CSV row."""

	rows = read_rows(predictions_csv)
	parsed: List[Dict[str, Any]] = []
	for row in rows:
		day_text = row.get("date", "").strip()
		if not day_text:
			continue
		try:
			day = parse_date(day_text)
		except ValueError:
			continue
		parsed.append({"date": day, "row": row})
	return parsed


def collect_latest_rows(
	history_rows: Sequence[Tuple[date, Dict[str, Any]]],
	target_date: date,
	count: int,
) -> List[Tuple[date, Dict[str, Any]]]:
	"""Return the most recent `count` historic rows before the target date."""

	eligible = [(day, row) for day, row in history_rows if day < target_date]
	return eligible[-count:]


def collect_seasonal_rows(
	history_rows: Sequence[Tuple[date, Dict[str, Any]]],
	*,
	target_date: date,
	window_days: int,
	) -> List[Tuple[date, Dict[str, Any]]]:
	"""Collect rows from a seasonal window around the target day-of-year.

	Uses circular day-of-year distance so dates near year boundaries still match
	(e.g. late December and early January).
	"""

	target_day_of_year = target_date.timetuple().tm_yday
	rows: List[Tuple[date, Dict[str, Any]]] = []
	for day, row in history_rows:
		if day >= target_date:
			continue
		day_of_year = day.timetuple().tm_yday
		circular_gap = min(abs(day_of_year - target_day_of_year), 365 - abs(day_of_year - target_day_of_year))
		if circular_gap <= window_days:
			rows.append((day, row))
	return rows


def collect_numeric_values(rows: Sequence[Tuple[date, Dict[str, Any]]], field: str) -> List[float]:
	"""Extract numeric values for one field from preselected row subsets."""

	values: List[float] = []
	for _, row in rows:
		value = to_float(row.get(field))
		if value is not None:
			values.append(value)
	return values


def circular_day_gap(target: date, candidate: date) -> int:
	"""Return circular day-of-year gap so year-end dates compare correctly."""

	target_doy = target.timetuple().tm_yday
	candidate_doy = candidate.timetuple().tm_yday
	raw_gap = abs(target_doy - candidate_doy)
	return min(raw_gap, 365 - raw_gap)


def numeric_pattern_distance(
	prediction_row: Dict[str, Any],
	history_row: Dict[str, Any],
	fields: Sequence[str],
) -> Tuple[float, int]:
	"""Compute normalized distance between two weather rows.

	Distance is the mean of per-field normalized errors. Lower is better.
	Returns (distance, number_of_fields_used).
	"""

	components: List[float] = []
	for field in fields:
		predicted = to_float(prediction_row.get(field))
		historic = to_float(history_row.get(field))
		if predicted is None or historic is None:
			continue
		denominator = max(abs(historic), 1.0)
		components.append(abs(predicted - historic) / denominator)

	if not components:
		return (float("inf"), 0)

	distance = mean(components)
	predicted_weather = str(prediction_row.get("weather_main", "")).strip().lower()
	historic_weather = str(history_row.get("weather_main", "")).strip().lower()
	if predicted_weather and historic_weather and predicted_weather == historic_weather:
		# Small bonus when broad weather type also matches.
		distance *= 0.85

	return (distance, len(components))


def find_closest_pattern_matches(
	history_rows: Sequence[Tuple[date, Dict[str, Any]]],
	prediction_day: date,
	prediction_row: Dict[str, Any],
	*,
	window_days: int,
	top_n: Optional[int] = None,
) -> List[PatternMatch]:
	"""Find closest seasonal historic matches across all prior years in history.

	When `top_n` is provided, this uses a top-k selection strategy instead of a
	full sort for better scalability.
	"""

	candidates: List[PatternMatch] = []
	for historic_day, historic_row in history_rows:
		if historic_day >= prediction_day:
			continue

		day_gap = circular_day_gap(prediction_day, historic_day)
		if day_gap > window_days:
			continue

		distance, fields_used = numeric_pattern_distance(
			prediction_row,
			historic_row,
			RELIABILITY_NUMERIC_FIELDS,
		)
		if fields_used == 0:
			continue

		candidates.append(
			PatternMatch(
				matched_date=historic_day,
				distance=distance,
				day_gap=day_gap,
				fields_used=fields_used,
			)
		)

	if top_n is None:
		candidates.sort(key=lambda item: (item.distance, item.day_gap, item.matched_date))
		return candidates

	return nsmallest(
		max(top_n, 1),
		candidates,
		key=lambda item: (item.distance, item.day_gap, item.matched_date),
	)


def format_match_line(match: PatternMatch, range_days: int) -> str:
	"""Format one historic match with centered date range for manual checks."""

	start = match.matched_date - timedelta(days=range_days)
	end = match.matched_date + timedelta(days=range_days)
	return (
		f"{start.isoformat()}..{end.isoformat()} "
		f"(center={match.matched_date.isoformat()}, dist={match.distance:.3f}, "
		f"gap={match.day_gap}d, fields={match.fields_used})"
	)


def score_numeric_consistency(predicted: Optional[float], baseline: Sequence[float]) -> float:
	"""Score how typical a prediction is against baseline values.

	Returns 0..1 where 1 is very close to baseline center and 0 is far outside.
	A z-score style approach is used for stable behavior across fields.
	"""

	if predicted is None or not baseline:
		return 0.0
	center = mean(baseline)
	spread = pstdev(baseline) if len(baseline) > 1 else 0.0
	if spread == 0:
		spread = max(abs(center) * 0.15, 1.0)
	z_score = abs(predicted - center) / spread
	return max(0.0, 1.0 - min(z_score / 3.0, 1.0))


def score_direction_alignment(recent_actuals: Sequence[float], predicted: Optional[float]) -> float:
	"""Score whether the predicted next value matches recent direction of change.

	Returns:
	- 1.0 for direction match,
	- 0.25 for mismatch,
	- 0.5 for flat/neutral cases,
	- 0.0 when insufficient data.
	"""

	if predicted is None or len(recent_actuals) < 3:
		return 0.0
	recent_change = recent_actuals[-1] - recent_actuals[0]
	recent_direction = 1 if recent_change > 0 else -1 if recent_change < 0 else 0
	recent_mean = mean(recent_actuals[-3:])
	next_direction = 1 if predicted > recent_mean else -1 if predicted < recent_mean else 0
	if recent_direction == 0 or next_direction == 0:
		return 0.5
	return 1.0 if recent_direction == next_direction else 0.25


def score_weather_pattern(predicted_weather: str, historic_weather: Sequence[str]) -> float:
	"""Score how common the predicted weather category is in recent history."""

	if not predicted_weather or not historic_weather:
		return 0.0
	counts = Counter(value for value in historic_weather if value)
	if not counts:
		return 0.0
	most_common_weather, frequency = counts.most_common(1)[0]
	if predicted_weather == most_common_weather:
		return 1.0
	match_frequency = counts.get(predicted_weather, 0) / sum(counts.values())
	return match_frequency * 0.75


def summarize_notes(scores: Sequence[Tuple[str, float]]) -> str:
	"""Create compact key=value notes sorted from strongest to weakest signal."""

	#ordered = sorted(scores, key=lambda item: item[1], reverse=True)
	return ", ".join(f"{name}={score:.2f}" for name, score in scores)


def band_from_score(score: int) -> str:
	"""Map 0-100 reliability score into low/medium/high buckets."""

	if score >= 75:
		return "high"
	if score >= 50:
		return "medium"
	return "low"


def assess_prediction(history: Dict[date, Dict[str, Any]], prediction_day: date, prediction_row: Dict[str, Any]) -> Assessment:
	"""Backward-compatible wrapper that accepts date-keyed history."""

	return assess_prediction_with_rows(
		[(day, row) for day, row in history.items()],
		prediction_day,
		prediction_row,
	)


def assess_prediction_with_rows(
	history_rows: Sequence[Tuple[date, Dict[str, Any]]],
	prediction_day: date,
	prediction_row: Dict[str, Any],
) -> Assessment:
	"""Evaluate one prediction against recent and seasonal historic patterns."""

	numeric_scores: List[Tuple[str, float]] = []
	trend_scores: List[Tuple[str, float]] = []
	recent_rows = collect_latest_rows(history_rows, prediction_day, 30)
	seasonal_rows = collect_seasonal_rows(history_rows, target_date=prediction_day, window_days=21)

	# Numeric fields are checked against two baselines:
	# 1) recent baseline (latest observations), and
	# 2) seasonal baseline (similar time of year).
	for field in RELIABILITY_NUMERIC_FIELDS:
		recent_baseline = collect_numeric_values(recent_rows, field)
		seasonal_baseline = collect_numeric_values(seasonal_rows, field)
		predicted_value = to_float(prediction_row.get(field))
		recent_score = score_numeric_consistency(predicted_value, recent_baseline)
		seasonal_score = score_numeric_consistency(predicted_value, seasonal_baseline)
		score = mean([recent_score, seasonal_score]) if (recent_baseline or seasonal_baseline) else 0.0
		if (recent_baseline or seasonal_baseline) and predicted_value is not None:
			numeric_scores.append((field, score))

	recent_days = recent_rows[-7:]
	recent_temp_values = [to_float(row.get("temp_day")) for _, row in recent_days]
	recent_temp_values = [value for value in recent_temp_values if value is not None]
	trend_alignment = score_direction_alignment(recent_temp_values, to_float(prediction_row.get("temp_day")))
	trend_scores.append(("temp_trend", trend_alignment))

	historic_weather = []
	for day, row in history_rows:
		if prediction_day - timedelta(days=90) <= day < prediction_day:
			value = str(row.get("weather_main", "")).strip()
			if value:
				historic_weather.append(value)
	seasonal_alignment = score_weather_pattern(str(prediction_row.get("weather_main", "")).strip(), historic_weather)
	trend_scores.append(("weather_pattern", seasonal_alignment))

	numeric_average = mean(score for _, score in numeric_scores) if numeric_scores else 0.0
	trend_average = mean(score for _, score in trend_scores) if trend_scores else 0.0

	# Weighted blend prioritizes numeric realism while still considering pattern fit.
	combined = (numeric_average * 0.65) + (trend_average * 0.35)
	if not numeric_scores:
		# Small confidence penalty if no numeric fields were usable.
		combined *= 0.85

	reliability_score = max(0, min(100, round(combined * 100)))
	reliability_band = band_from_score(reliability_score)

	notes = summarize_notes(
		[
			("numeric", numeric_average),
			("trend", trend_alignment),
			("seasonal", seasonal_alignment),
		]
	)

	return Assessment(
		prediction_date=prediction_day.isoformat(),
		reliability_score=reliability_score,
		reliability_band=reliability_band,
		trend_alignment=trend_alignment,
		seasonal_alignment=seasonal_alignment,
		notes=notes,
	)


def write_report_csv(output_path: Path, assessments: Sequence[Assessment]) -> None:
	"""Write analysis results to a report CSV for later review."""

	output_path.parent.mkdir(parents=True, exist_ok=True)
	with output_path.open("w", newline="", encoding="utf-8") as handle:
		writer = csv.DictWriter(
			handle,
			fieldnames=[
				"date",
				"reliability_score",
				"reliability_band",
				"trend_alignment",
				"seasonal_alignment",
				"notes",
			],
		)
		writer.writeheader()
		for item in assessments:
			writer.writerow(
				{
					"date": item.prediction_date,
					"reliability_score": item.reliability_score,
					"reliability_band": item.reliability_band,
					"trend_alignment": f"{item.trend_alignment:.2f}",
					"seasonal_alignment": f"{item.seasonal_alignment:.2f}",
					"notes": item.notes,
				}
			)


def build_arg_parser() -> argparse.ArgumentParser:
	"""Define CLI arguments for running trend reliability analysis."""

	parser = argparse.ArgumentParser(
		description="Compare forecast rows against historic weather trends and estimate reliability."
	)
	parser.add_argument(
		"--actuals-csv",
		default="weather_actuals.csv",
		help="Observed history CSV path (default: weather_actuals.csv).",
	)
	parser.add_argument(
		"--predictions-csv",
		default="weather_predictions.csv",
		help="Forecast CSV path (default: weather_predictions.csv).",
	)
	parser.add_argument(
		"--output-csv",
		default=None,
		help="Optional CSV file for the analysis report.",
	)
	parser.add_argument(
		"--show-pattern-matches",
		action="store_true",
		help="Print closest historic seasonal matches for each prediction day.",
	)
	parser.add_argument(
		"--pattern-top-n",
		type=int,
		default=5,
		help="How many closest overall seasonal matches to print (default: 5).",
	)
	parser.add_argument(
		"--pattern-window-days",
		type=int,
		default=35,
		help="Day-of-year seasonal window for pattern search (default: 35).",
	)
	parser.add_argument(
		"--pattern-range-days",
		type=int,
		default=3,
		help="How many days before/after matched date to print as a range (default: 3).",
	)
	return parser


def main() -> None:
	"""CLI entrypoint: load CSVs, score predictions, print and optionally export."""

	parser = build_arg_parser()
	args = parser.parse_args()

	actuals_csv = Path(args.actuals_csv)
	predictions_csv = Path(args.predictions_csv)
	if not actuals_csv.exists():
		parser.error(f"Actuals CSV not found: {actuals_csv}")
	if not predictions_csv.exists():
		parser.error(f"Predictions CSV not found: {predictions_csv}")

	history_rows = load_history_rows(actuals_csv)
	predictions = load_predictions(predictions_csv)
	assessments = [assess_prediction_with_rows(history_rows, item["date"], item["row"]) for item in predictions]

	if args.output_csv:
		write_report_csv(Path(args.output_csv), assessments)

	for prediction_item, item in zip(predictions, assessments):
		print(
			f"{item.prediction_date} | reliability={item.reliability_score:>3} ({item.reliability_band}) | "
			f"{item.notes}"
		)

		if args.show_pattern_matches:
			top_n = max(args.pattern_top_n, 1)
			matches = find_closest_pattern_matches(
				history_rows,
				prediction_item["date"],
				prediction_item["row"],
				window_days=args.pattern_window_days,
				top_n=top_n,
			)
			if not matches:
				print("  pattern matches: no eligible historic rows in selected seasonal window")
				continue

			print(f"  pattern matches (overall top {top_n}):")
			for match in matches:
				print(f"    - {format_match_line(match, range_days=max(args.pattern_range_days, 0))}")


if __name__ == "__main__":
	main()
