"""Data ingestion module for fetching SF open datasets via Socrata API.

Downloads building permits, affordable housing pipeline, and development
pipeline datasets from data.sfgov.org, with pagination, caching, and retries.
"""

import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from sodapy import Socrata

from src.config import DATASETS, RAW_DIR, SOCRATA_DOMAIN

# Pagination chunk size for large datasets
_CHUNK_SIZE = 50_000

# Cache TTL: skip re-download if parquet file is less than 24 hours old
_CACHE_TTL = timedelta(hours=24)

# Retry settings for network errors
_MAX_RETRIES = 3
_INITIAL_BACKOFF_SECONDS = 2


def _cache_path(dataset_key: str) -> Path:
    """Return the parquet cache file path for a given dataset key."""
    return RAW_DIR / f"{dataset_key}.parquet"


def _cache_is_fresh(path: Path) -> bool:
    """Check whether a cached parquet file exists and is less than 24 hours old."""
    if not path.exists():
        return False
    mtime = datetime.fromtimestamp(path.stat().st_mtime)
    return (datetime.now() - mtime) < _CACHE_TTL


def _fetch_with_retries(client: Socrata, dataset_id: str, limit: int, offset: int) -> list:
    """Fetch a single page from Socrata with exponential-backoff retries.

    Parameters
    ----------
    client : Socrata
        An authenticated (or public) Socrata client instance.
    dataset_id : str
        The Socrata dataset identifier (e.g. "i98e-djp9").
    limit : int
        Number of rows to request per page.
    offset : int
        Row offset for pagination.

    Returns
    -------
    list[dict]
        A list of row dictionaries returned by the API.

    Raises
    ------
    Exception
        Re-raises the last exception after all retries are exhausted.
    """
    backoff = _INITIAL_BACKOFF_SECONDS
    last_exception: BaseException | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            return client.get(dataset_id, limit=limit, offset=offset)  # type: ignore[no-any-return]
        except Exception as exc:
            last_exception = exc
            if attempt < _MAX_RETRIES:
                print(
                    f"  Retry {attempt}/{_MAX_RETRIES} after error: {exc}. "
                    f"Waiting {backoff}s..."
                )
                time.sleep(backoff)
                backoff *= 2
            else:
                print(f"  All {_MAX_RETRIES} attempts failed.")

    # last_exception is guaranteed to be set here because _MAX_RETRIES >= 1
    raise last_exception  # type: ignore[misc]


def fetch_dataset(dataset_key: str, force_refresh: bool = False) -> pd.DataFrame:
    """Download a single dataset from Socrata, with caching and pagination.

    Parameters
    ----------
    dataset_key : str
        One of the keys in ``DATASETS`` (e.g. ``"building_permits"``).
    force_refresh : bool, optional
        If True, ignore the cache and re-download regardless of age.

    Returns
    -------
    pd.DataFrame
        The full dataset as a pandas DataFrame.

    Raises
    ------
    KeyError
        If *dataset_key* is not found in ``DATASETS``.
    """
    if dataset_key not in DATASETS:
        raise KeyError(
            f"Unknown dataset key '{dataset_key}'. "
            f"Valid keys: {list(DATASETS.keys())}"
        )

    cache_file = _cache_path(dataset_key)

    # Return cached data if it is fresh and no forced refresh was requested
    if not force_refresh and _cache_is_fresh(cache_file):
        print(f"Using cached {dataset_key} ({cache_file})")
        return pd.read_parquet(cache_file)

    dataset_id = DATASETS[dataset_key]["id"]
    print(f"Fetching {dataset_key} (dataset {dataset_id}) from {SOCRATA_DOMAIN}...")

    client = Socrata(SOCRATA_DOMAIN, None, timeout=60)

    all_rows: list[dict] = []
    offset = 0

    while True:
        page = _fetch_with_retries(client, dataset_id, limit=_CHUNK_SIZE, offset=offset)
        if not page:
            break
        all_rows.extend(page)
        print(f"  Fetching {dataset_key}: {len(all_rows)} rows downloaded so far...")
        offset += _CHUNK_SIZE

    client.close()

    df = pd.DataFrame.from_records(all_rows)
    print(f"  {dataset_key}: {len(df)} total rows, {len(df.columns)} columns")

    # Persist to parquet cache
    df.to_parquet(cache_file, index=False)
    print(f"  Cached to {cache_file}")

    return df


def fetch_all(force_refresh: bool = False) -> dict[str, pd.DataFrame]:
    """Download all configured datasets.

    Parameters
    ----------
    force_refresh : bool, optional
        If True, ignore the cache and re-download all datasets.

    Returns
    -------
    dict[str, pd.DataFrame]
        A mapping of dataset key to its DataFrame.
    """
    results: dict[str, pd.DataFrame] = {}
    for key in DATASETS:
        results[key] = fetch_dataset(key, force_refresh=force_refresh)
    return results


if __name__ == "__main__":
    fetch_all()
    print("All datasets downloaded successfully")
