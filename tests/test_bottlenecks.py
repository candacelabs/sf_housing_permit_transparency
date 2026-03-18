"""Tests for the bottleneck analysis module (src.analysis.bottlenecks)."""

import pandas as pd
import pytest

from src.analysis.bottlenecks import (
    DURATION_COLUMNS,
    district_scorecard,
    permit_status_breakdown,
    stage_duration_summary,
    stuck_permits,
    volume_analysis,
    worst_bottlenecks,
)


# ---------------------------------------------------------------------------
# stage_duration_summary
# ---------------------------------------------------------------------------


class TestStageDurationSummary:
    def test_overall_returns_stats_for_all_stages(self, sample_clean_permits: pd.DataFrame):
        result = stage_duration_summary(sample_clean_permits)
        assert isinstance(result, pd.DataFrame)
        assert "stage" in result.columns
        # Should have a row for each present duration column
        present = [c for c in DURATION_COLUMNS if c in sample_clean_permits.columns]
        assert set(result["stage"].tolist()) == set(present)

    def test_overall_has_expected_stat_columns(self, sample_clean_permits: pd.DataFrame):
        result = stage_duration_summary(sample_clean_permits)
        for col in ["median", "mean", "p25", "p75", "p90"]:
            assert col in result.columns, f"Missing stat column: {col}"

    def test_grouped_returns_per_district_stats(self, sample_clean_permits: pd.DataFrame):
        result = stage_duration_summary(
            sample_clean_permits, group_by="supervisor_district"
        )
        assert isinstance(result, pd.DataFrame)
        assert "supervisor_district" in result.columns
        assert "stage" in result.columns
        # Should have multiple districts
        assert result["supervisor_district"].nunique() > 1

    def test_median_is_non_negative(self, sample_clean_permits: pd.DataFrame):
        result = stage_duration_summary(sample_clean_permits)
        non_null = result["median"].dropna()
        assert (non_null >= 0).all()


# ---------------------------------------------------------------------------
# worst_bottlenecks
# ---------------------------------------------------------------------------


class TestWorstBottlenecks:
    def test_returns_sorted_results(self, sample_clean_permits: pd.DataFrame):
        result = worst_bottlenecks(sample_clean_permits, top_n=5)
        assert isinstance(result, pd.DataFrame)
        medians = result["median"].dropna().tolist()
        # Should be sorted descending
        assert medians == sorted(medians, reverse=True)

    def test_respects_top_n(self, sample_clean_permits: pd.DataFrame):
        result = worst_bottlenecks(sample_clean_permits, top_n=3)
        assert len(result) <= 3

    def test_has_count_column(self, sample_clean_permits: pd.DataFrame):
        result = worst_bottlenecks(sample_clean_permits)
        assert "count" in result.columns


# ---------------------------------------------------------------------------
# permit_status_breakdown
# ---------------------------------------------------------------------------


class TestPermitStatusBreakdown:
    def test_counts_and_pcts_sum_correctly(self, sample_clean_permits: pd.DataFrame):
        result = permit_status_breakdown(sample_clean_permits)
        assert "count" in result.columns
        assert "pct" in result.columns
        # Total count should equal number of rows
        assert result["count"].sum() == len(sample_clean_permits)
        # Percentages should sum to ~100
        assert abs(result["pct"].sum() - 100.0) < 0.1

    def test_grouped_pcts_sum_per_group(self, sample_clean_permits: pd.DataFrame):
        result = permit_status_breakdown(
            sample_clean_permits, group_by="filed_year"
        )
        for year, grp in result.groupby("filed_year"):
            assert abs(grp["pct"].sum() - 100.0) < 0.1, (
                f"Percentages for year {year} don't sum to 100"
            )

    def test_sorted_by_count_desc(self, sample_clean_permits: pd.DataFrame):
        result = permit_status_breakdown(sample_clean_permits)
        counts = result["count"].tolist()
        assert counts == sorted(counts, reverse=True)


# ---------------------------------------------------------------------------
# stuck_permits
# ---------------------------------------------------------------------------


class TestStuckPermits:
    def test_only_filed_or_approved(self, sample_clean_permits: pd.DataFrame):
        result = stuck_permits(sample_clean_permits, threshold_days=1)
        if len(result) > 0:
            assert result["status"].str.lower().isin(["filed", "approved"]).all()

    def test_days_exceed_threshold(self, sample_clean_permits: pd.DataFrame):
        threshold = 365
        result = stuck_permits(sample_clean_permits, threshold_days=threshold)
        if len(result) > 0:
            assert (result["days_waiting"] > threshold).all()

    def test_sorted_by_days_waiting_desc(self, sample_clean_permits: pd.DataFrame):
        result = stuck_permits(sample_clean_permits, threshold_days=1)
        if len(result) > 1:
            days = result["days_waiting"].tolist()
            assert days == sorted(days, reverse=True)

    def test_returns_expected_columns(self, sample_clean_permits: pd.DataFrame):
        result = stuck_permits(sample_clean_permits, threshold_days=1)
        expected_cols = {"permit_number", "filed_date", "status", "days_waiting"}
        assert expected_cols.issubset(set(result.columns))

    def test_stuck_permits_found(self, sample_clean_permits: pd.DataFrame):
        """Our fixture has 15 stuck permits filed in 2022; they should appear."""
        result = stuck_permits(sample_clean_permits, threshold_days=365)
        assert len(result) > 0, "Expected stuck permits from 2022 fixture data"


# ---------------------------------------------------------------------------
# volume_analysis
# ---------------------------------------------------------------------------


class TestVolumeAnalysis:
    def test_returns_counts_per_year(self, sample_clean_permits: pd.DataFrame):
        result = volume_analysis(sample_clean_permits)
        assert isinstance(result, pd.DataFrame)
        assert "filed_year" in result.columns
        assert "permits_filed" in result.columns

    def test_has_issued_and_completed(self, sample_clean_permits: pd.DataFrame):
        result = volume_analysis(sample_clean_permits)
        assert "permits_issued" in result.columns
        assert "permits_completed" in result.columns

    def test_has_net_new_units(self, sample_clean_permits: pd.DataFrame):
        result = volume_analysis(sample_clean_permits)
        assert "net_new_units" in result.columns

    def test_sorted_by_year(self, sample_clean_permits: pd.DataFrame):
        result = volume_analysis(sample_clean_permits)
        years = result["filed_year"].tolist()
        assert years == sorted(years)


# ---------------------------------------------------------------------------
# district_scorecard
# ---------------------------------------------------------------------------


class TestDistrictScorecard:
    def test_one_row_per_district(self, sample_clean_permits: pd.DataFrame):
        result = district_scorecard(sample_clean_permits)
        assert isinstance(result, pd.DataFrame)
        assert "supervisor_district" in result.columns
        # Each district appears once
        assert result["supervisor_district"].is_unique

    def test_has_bottleneck_score(self, sample_clean_permits: pd.DataFrame):
        result = district_scorecard(sample_clean_permits)
        assert "bottleneck_score" in result.columns

    def test_has_expected_metrics(self, sample_clean_permits: pd.DataFrame):
        result = district_scorecard(sample_clean_permits)
        for col in [
            "median_days_to_issuance",
            "total_permits",
            "units_proposed",
            "units_stuck",
        ]:
            assert col in result.columns, f"Missing scorecard column: {col}"

    def test_sorted_by_bottleneck_score_desc(self, sample_clean_permits: pd.DataFrame):
        result = district_scorecard(sample_clean_permits)
        scores = result["bottleneck_score"].tolist()
        assert scores == sorted(scores, reverse=True)
