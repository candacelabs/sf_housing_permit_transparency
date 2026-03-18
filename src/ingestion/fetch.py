"""Data ingestion module for fetching SF open datasets.

Downloads building permits, affordable housing pipeline, and development
pipeline datasets from data.sfgov.org via direct CSV export (fast, single
request, no rate limits) with parquet caching.
"""

import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from src.config import DATASETS, RAW_DIR, SOCRATA_DOMAIN

logger = logging.getLogger(__name__)

# Cache TTL: skip re-download if parquet file is less than 24 hours old
_CACHE_TTL = timedelta(hours=24)

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


def _csv_export_url(dataset_id: str) -> str:
    """Build the direct CSV export URL for a DataSF dataset."""
    return f"https://{SOCRATA_DOMAIN}/api/views/{dataset_id}/rows.csv?accessType=DOWNLOAD"


def fetch_dataset(dataset_key: str, force_refresh: bool = False) -> pd.DataFrame:
    """Download a single dataset via CSV export, with caching and retries.

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
    """
    if dataset_key not in DATASETS:
        raise KeyError(
            f"Unknown dataset key '{dataset_key}'. "
            f"Valid keys: {list(DATASETS.keys())}"
        )

    cache_file = _cache_path(dataset_key)

    if not force_refresh and _cache_is_fresh(cache_file):
        logger.info("Using cached %s (%s)", dataset_key, cache_file)
        return pd.read_parquet(cache_file)

    dataset_id = DATASETS[dataset_key]["id"]
    url = _csv_export_url(dataset_id)
    logger.info("Downloading %s from %s ...", dataset_key, url)

    backoff = _INITIAL_BACKOFF_SECONDS
    last_exception: BaseException | None = None

    csv_file = RAW_DIR / f"{dataset_key}.csv"

    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            # Stream straight to disk — no memory buffering
            with requests.get(url, timeout=600, stream=True) as resp:
                resp.raise_for_status()
                downloaded = 0
                last_logged = 0
                with open(csv_file, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
                        mb = downloaded / 1e6
                        if mb - last_logged >= 25:
                            logger.info("  %s: %.0f MB downloaded...", dataset_key, mb)
                            last_logged = mb
            # Parse CSV from disk
            logger.info("  Parsing %s from disk...", dataset_key)
            df = pd.read_csv(csv_file, low_memory=False)
            logger.info("  %s: %s rows, %d columns", dataset_key, f"{len(df):,}", len(df.columns))
            # Save as parquet and remove CSV
            df.to_parquet(cache_file, index=False)
            csv_file.unlink(missing_ok=True)
            logger.info("  Cached to %s", cache_file)
            return df
        except Exception as exc:
            last_exception = exc
            csv_file.unlink(missing_ok=True)
            if attempt < _MAX_RETRIES:
                logger.warning("  Retry %d/%d after error: %s. Waiting %ds...", attempt, _MAX_RETRIES, exc, backoff)
                time.sleep(backoff)
                backoff *= 2
            else:
                logger.error("  All %d attempts failed.", _MAX_RETRIES)

    raise last_exception  # type: ignore[misc]


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
        try:
            results[key] = fetch_dataset(key, force_refresh=force_refresh)
        except Exception as exc:
            logger.warning("Skipping %s: %s", key, exc)
    return results


if __name__ == "__main__":
    fetch_all()
    logger.info("All datasets downloaded successfully")
