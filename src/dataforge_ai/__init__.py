"""DataForge AI -- reusable PySpark transformations for the NYC taxi pipeline.

Extracted from notebooks/01-04 once the same read/clean/join/aggregate logic
started being duplicated across every downstream notebook. Each notebook
still *teaches* the concept where it was introduced (see notebooks/02 for
clean_trips, notebooks/03 for join_zones); later notebooks import from here
instead of copy-pasting.
"""
from dataforge_ai.io import TARGET_TYPES, read_and_cast, read_trips
from dataforge_ai.cleaning import clean_trips
from dataforge_ai.joins import join_zones
from dataforge_ai.aggregations import hourly_demand, rank_zones_by_borough

__all__ = [
    "TARGET_TYPES",
    "read_and_cast",
    "read_trips",
    "clean_trips",
    "join_zones",
    "hourly_demand",
    "rank_zones_by_borough",
]
