"""Tests for the data cleaning module (src.ingestion.clean)."""

import pandas as pd
import pytest

from src.ingestion.clean import clean_building_permits


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------


class TestDateParsing:
    """All date columns should be datetime64 after cleaning."""

    DATE_COLUMNS = [
        "filed_date",
        "approved_date",
        "issued_date",
        "first_construction_document_date",
        "completed_date",
        "status_date",
        "permit_creation_date",
    ]

    def test_date_columns_are_datetime(self, sample_clean_permits: pd.DataFrame):
        for col in self.DATE_COLUMNS:
            assert pd.api.types.is_datetime64_any_dtype(sample_clean_permits[col]), (
                f"{col} should be datetime64, got {sample_clean_permits[col].dtype}"
            )

    def test_dates_not_all_null(self, sample_clean_permits: pd.DataFrame):
        """filed_date should have no NaT values since every row has one."""
        assert sample_clean_permits["filed_date"].notna().all()


# ---------------------------------------------------------------------------
# Cost parsing
# ---------------------------------------------------------------------------


class TestCostParsing:
    def test_estimated_cost_is_numeric(self, sample_clean_permits: pd.DataFrame):
        assert pd.api.types.is_numeric_dtype(sample_clean_permits["estimated_cost"])

    def test_cost_no_dollar_signs(self, sample_raw_permits: pd.DataFrame):
        """After cleaning, cost column must not contain $ or commas."""
        cleaned = clean_building_permits(sample_raw_permits)
        vals = cleaned["estimated_cost"].dropna().astype(str)
        assert not vals.str.contains(r"[\$,]").any()

    def test_cost_values_positive(self, sample_clean_permits: pd.DataFrame):
        non_null = sample_clean_permits["estimated_cost"].dropna()
        assert (non_null >= 0).all()


# ---------------------------------------------------------------------------
# Unit parsing
# ---------------------------------------------------------------------------


class TestUnitParsing:
    def test_existing_units_numeric(self, sample_clean_permits: pd.DataFrame):
        assert pd.api.types.is_numeric_dtype(sample_clean_permits["existing_units"])

    def test_proposed_units_numeric(self, sample_clean_permits: pd.DataFrame):
        assert pd.api.types.is_numeric_dtype(sample_clean_permits["proposed_units"])

    def test_net_new_units_computed(self, sample_clean_permits: pd.DataFrame):
        """net_new_units == proposed_units - existing_units."""
        df = sample_clean_permits.dropna(subset=["existing_units", "proposed_units"])
        expected = df["proposed_units"] - df["existing_units"]
        pd.testing.assert_series_equal(
            df["net_new_units"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )


# ---------------------------------------------------------------------------
# Status normalization
# ---------------------------------------------------------------------------


class TestStatusNormalization:
    def test_status_title_case(self, sample_clean_permits: pd.DataFrame):
        """All mapped status values should be title case."""
        valid_statuses = {
            "Filed", "Approved", "Issued", "Complete", "Cancelled",
            "Disapproved", "Expired", "Incomplete", "Plan Check",
            "Reinstated", "Revoked", "Suspended", "Withdrawn",
        }
        unique_statuses = set(sample_clean_permits["status"].dropna().unique())
        assert unique_statuses.issubset(valid_statuses), (
            f"Unexpected statuses: {unique_statuses - valid_statuses}"
        )


# ---------------------------------------------------------------------------
# Duration columns
# ---------------------------------------------------------------------------


class TestDurationColumns:
    DURATION_COLS = [
        "days_filed_to_approved",
        "days_approved_to_issued",
        "days_filed_to_issued",
        "days_issued_to_first_construction",
        "days_issued_to_completed",
        "days_total",
    ]

    def test_duration_columns_exist(self, sample_clean_permits: pd.DataFrame):
        for col in self.DURATION_COLS:
            assert col in sample_clean_permits.columns, f"Missing duration column: {col}"

    def test_duration_values_positive(self, sample_clean_permits: pd.DataFrame):
        """No negative durations should exist."""
        for col in self.DURATION_COLS:
            non_null = sample_clean_permits[col].dropna()
            if len(non_null) > 0:
                assert (non_null >= 0).all(), f"{col} contains negative values"

    def test_duration_cap(self, sample_clean_permits: pd.DataFrame):
        """No durations should exceed 7300 days."""
        for col in self.DURATION_COLS:
            non_null = sample_clean_permits[col].dropna()
            if len(non_null) > 0:
                assert (non_null <= 7300).all(), f"{col} exceeds 7300-day cap"


# ---------------------------------------------------------------------------
# Housing flag
# ---------------------------------------------------------------------------


class TestHousingFlag:
    def test_housing_flag_exists(self, sample_clean_permits: pd.DataFrame):
        assert "is_housing" in sample_clean_permits.columns

    def test_housing_flag_boolean(self, sample_clean_permits: pd.DataFrame):
        assert sample_clean_permits["is_housing"].dtype == bool

    def test_housing_true_for_apartments(self, sample_clean_permits: pd.DataFrame):
        """Permits with proposed_use containing 'apartments' should be housing."""
        apt_mask = sample_clean_permits["proposed_use"].str.lower().str.contains(
            "apartments", na=False
        )
        assert sample_clean_permits.loc[apt_mask, "is_housing"].all()

    def test_housing_true_for_residential(self, sample_clean_permits: pd.DataFrame):
        """Permits with proposed_use containing 'residential' should be housing."""
        res_mask = sample_clean_permits["proposed_use"].str.lower().str.contains(
            "residential", na=False
        )
        assert sample_clean_permits.loc[res_mask, "is_housing"].all()

    def test_housing_true_for_housing_permit_types(self, sample_clean_permits: pd.DataFrame):
        """Permit types 1, 2, 3, 8 should be flagged as housing."""
        type_mask = sample_clean_permits["permit_type"].isin([1, 2, 3, 8])
        assert sample_clean_permits.loc[type_mask, "is_housing"].all()

    def test_housing_true_for_positive_proposed_units(self, sample_clean_permits: pd.DataFrame):
        """Permits with proposed_units > 0 should be housing."""
        units_mask = sample_clean_permits["proposed_units"].fillna(0) > 0
        assert sample_clean_permits.loc[units_mask, "is_housing"].all()


# ---------------------------------------------------------------------------
# Filed year and quarter
# ---------------------------------------------------------------------------


class TestFiledYearQuarter:
    def test_filed_year_computed(self, sample_clean_permits: pd.DataFrame):
        assert "filed_year" in sample_clean_permits.columns
        non_null = sample_clean_permits.dropna(subset=["filed_date", "filed_year"])
        expected = non_null["filed_date"].dt.year
        pd.testing.assert_series_equal(
            non_null["filed_year"].astype(int).reset_index(drop=True),
            expected.astype(int).reset_index(drop=True),
            check_names=False,
        )

    def test_filed_quarter_computed(self, sample_clean_permits: pd.DataFrame):
        assert "filed_quarter" in sample_clean_permits.columns
        non_null = sample_clean_permits.dropna(subset=["filed_date", "filed_quarter"])
        expected = non_null["filed_date"].dt.quarter
        pd.testing.assert_series_equal(
            non_null["filed_quarter"].astype(int).reset_index(drop=True),
            expected.astype(int).reset_index(drop=True),
            check_names=False,
        )
