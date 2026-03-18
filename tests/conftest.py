"""Shared fixtures for SF Permitting Bottleneck Analyzer tests."""

import pytest
import pandas as pd

from src.ingestion.clean import clean_building_permits


# ---------------------------------------------------------------------------
# Helpers to build realistic raw data
# ---------------------------------------------------------------------------

_NEIGHBORHOODS = [
    "Mission", "SoMa", "Pacific Heights", "Tenderloin", "Sunset",
    "Richmond", "Noe Valley", "Castro", "Bayview", "Excelsior",
]

_STATUSES = ["filed", "approved", "issued", "complete", "cancelled"]

_PROPOSED_USES = [
    "apartments", "1 family dwelling", "office", "retail",
    "2 family dwelling", "residential hotel", "food/beverage hndlng",
    "apartments", "residential", "warehouse",
]


def _make_raw_rows() -> list[dict]:
    """Build ~50 raw permit rows with realistic Socrata-style data."""
    rows: list[dict] = []
    base_permit = 202001000

    # --- Group 1: 20 permits with full lifecycle (filed -> completed) ---
    for i in range(20):
        district = str((i % 11) + 1)
        permit_type = [1, 2, 3, 5, 8][i % 5]
        filed = f"20{20 + (i % 3):02d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T00:00:00.000"
        # Approvals ~30-120 days after filing
        approved = f"20{20 + (i % 3):02d}-{((i % 12) + 3) % 12 + 1:02d}-{(i % 28) + 1:02d}T00:00:00.000"
        issued = f"20{21 + (i % 3):02d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T00:00:00.000"
        completed = f"20{22 + (i % 3):02d}-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T00:00:00.000"

        cost_val = 50000 + i * 75000
        cost_str = f"${cost_val:,.0f}"

        rows.append({
            "permit_number": str(base_permit + i),
            "permit_type": str(permit_type),
            "status": ["complete", "issued", "complete", "issued", "complete"][i % 5],
            "filed_date": filed,
            "approved_date": approved,
            "issued_date": issued,
            "completed_date": completed,
            "first_construction_document_date": issued,
            "status_date": completed,
            "permit_creation_date": filed,
            "estimated_cost": cost_str,
            "revised_cost": cost_str,
            "existing_units": str(i % 4),
            "proposed_units": str((i % 4) + 5),
            "supervisor_district": district,
            "neighborhoods_analysis_boundaries": _NEIGHBORHOODS[i % len(_NEIGHBORHOODS)],
            "existing_use": "office",
            "proposed_use": _PROPOSED_USES[i % len(_PROPOSED_USES)],
            "description": f"Test permit {i} full lifecycle",
        })

    # --- Group 2: 15 permits that are "stuck" (filed in 2022, status=filed, no issued_date) ---
    for i in range(15):
        district = str((i % 11) + 1)
        permit_type = [1, 2, 3, 8, 1][i % 5]
        filed = f"2022-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T00:00:00.000"

        cost_val = 150000 + i * 200000
        cost_str = f"${cost_val:,.0f}"

        rows.append({
            "permit_number": str(base_permit + 20 + i),
            "permit_type": str(permit_type),
            "status": "filed" if i % 3 != 2 else "approved",
            "filed_date": filed,
            "approved_date": f"2022-{(i % 12) + 1:02d}-15T00:00:00.000" if i % 3 == 2 else None,
            "issued_date": None,
            "completed_date": None,
            "first_construction_document_date": None,
            "status_date": filed,
            "permit_creation_date": filed,
            "estimated_cost": cost_str,
            "revised_cost": None,
            "existing_units": "0",
            "proposed_units": str((i % 10) + 1),
            "supervisor_district": district,
            "neighborhoods_analysis_boundaries": _NEIGHBORHOODS[i % len(_NEIGHBORHOODS)],
            "existing_use": "vacant lot",
            "proposed_use": _PROPOSED_USES[i % len(_PROPOSED_USES)],
            "description": f"Test permit {20 + i} stuck",
        })

    # --- Group 3: 10 cancelled permits ---
    for i in range(10):
        district = str((i % 11) + 1)
        permit_type = [1, 2, 5, 3, 8][i % 5]
        filed = f"2021-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T00:00:00.000"

        rows.append({
            "permit_number": str(base_permit + 35 + i),
            "permit_type": str(permit_type),
            "status": "cancelled",
            "filed_date": filed,
            "approved_date": None,
            "issued_date": None,
            "completed_date": None,
            "first_construction_document_date": None,
            "status_date": filed,
            "permit_creation_date": filed,
            "estimated_cost": "$2,500,000",
            "revised_cost": None,
            "existing_units": "0",
            "proposed_units": "0",
            "supervisor_district": district,
            "neighborhoods_analysis_boundaries": _NEIGHBORHOODS[i % len(_NEIGHBORHOODS)],
            "existing_use": "office",
            "proposed_use": "office",
            "description": f"Test permit {35 + i} cancelled",
        })

    # --- Group 4: 5 permits with apartments/residential proposed_use and housing permit types ---
    for i in range(5):
        district = str((i % 5) + 1)
        filed = f"2023-{(i % 6) + 1:02d}-15T00:00:00.000"
        approved = f"2023-{(i % 6) + 4:02d}-15T00:00:00.000"
        issued = f"2024-01-15T00:00:00.000"

        rows.append({
            "permit_number": str(base_permit + 45 + i),
            "permit_type": "1",
            "status": "issued",
            "filed_date": filed,
            "approved_date": approved,
            "issued_date": issued,
            "completed_date": None,
            "first_construction_document_date": issued,
            "status_date": issued,
            "permit_creation_date": filed,
            "estimated_cost": "$5,000,000",
            "revised_cost": "$6,000,000",
            "existing_units": "0",
            "proposed_units": "50",
            "supervisor_district": district,
            "neighborhoods_analysis_boundaries": _NEIGHBORHOODS[i % len(_NEIGHBORHOODS)],
            "existing_use": "vacant lot",
            "proposed_use": "apartments" if i % 2 == 0 else "residential",
            "description": f"Test permit {45 + i} housing",
        })

    return rows


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_raw_permits() -> pd.DataFrame:
    """A DataFrame of ~50 raw permit rows with realistic Socrata-style data."""
    return pd.DataFrame(_make_raw_rows())


@pytest.fixture()
def sample_clean_permits(sample_raw_permits: pd.DataFrame) -> pd.DataFrame:
    """Cleaned version of sample_raw_permits."""
    return clean_building_permits(sample_raw_permits)
