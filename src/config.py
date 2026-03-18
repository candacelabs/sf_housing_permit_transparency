"""Configuration for the SF Permitting Bottleneck Analyzer."""
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
REPORTS_DIR = PROJECT_ROOT / "reports"

# Ensure directories exist
for d in [RAW_DIR, PROCESSED_DIR, REPORTS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# DataSF Socrata endpoints (no app token needed for public data, but rate-limited)
SOCRATA_DOMAIN = "data.sfgov.org"
DATASETS = {
    "building_permits": {
        "id": "i98e-djp9",
        "description": "DBI Building Permits (1M+ rows, back to 1980s)",
    },
    "affordable_housing": {
        "id": "aaxw-2cb8",
        "description": "MOHCD Affordable Housing Pipeline (194 projects)",
    },
    "development_pipeline": {
        "id": "k55i-dnjd",
        "description": "SF Development Pipeline (quarterly snapshots)",
    },
}

# Permit types relevant to housing
HOUSING_PERMIT_TYPES = [1, 2, 3, 8]  # 1=new construction, 2=additions, 3=alterations, 8=demolitions

# Key date columns for stage duration analysis
STAGE_DATE_COLUMNS = [
    "filed_date",
    "approved_date",
    "issued_date",
    "first_construction_document_date",
    "completed_date",
]

# Permit statuses
STATUS_MAPPING = {
    "filed": "Filed",
    "approved": "Approved",
    "issued": "Issued",
    "complete": "Complete",
    "cancelled": "Cancelled",
    "disapproved": "Disapproved",
    "expired": "Expired",
    "incomplete": "Incomplete",
    "plancheck": "Plan Check",
    "reinstated": "Reinstated",
    "revoked": "Revoked",
    "suspended": "Suspended",
    "withdrawn": "Withdrawn",
}

# Key policy dates for trend annotations
POLICY_MILESTONES = {
    "2020-03-17": "COVID-19 shelter-in-place",
    "2023-01-01": "AB 2011 takes effect (ministerial housing approvals)",
    "2023-10-01": "SB 423 takes effect (streamlined approvals extension)",
    "2024-01-01": "Builder's Remedy period begins",
    "2025-01-08": "Mayor Lurie takes office",
    "2026-01-01": "Lurie announces permitting consolidation",
}

# Dashboard settings
DASH_HOST = os.environ.get("DASH_HOST", "127.0.0.1")
DASH_PORT = 8050
DASH_DEBUG = True
