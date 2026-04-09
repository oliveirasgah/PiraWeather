"""Tests for dashboard/app.py — pure helpers and API client."""

from unittest.mock import MagicMock, patch

import pandas as pd

from dashboard.app import ERA_LABELS, api_get, fetch_bronze_sample, fetch_bronze_stats, get_era


# ---------------------------------------------------------------------------
# get_era
# ---------------------------------------------------------------------------


class TestGetEra:
    def test_era1_lower_bound(self):
        assert get_era(1997) == "Era 1 (1997–2002)"

    def test_era1_upper_bound(self):
        assert get_era(2002) == "Era 1 (1997–2002)"

    def test_era2_lower_bound(self):
        assert get_era(2003) == "Era 2 (2003–2016)"

    def test_era2_upper_bound(self):
        assert get_era(2016) == "Era 2 (2003–2016)"

    def test_era3_lower_bound(self):
        assert get_era(2017) == "Era 3 (2017–2023)"

    def test_era3_upper_bound(self):
        assert get_era(2023) == "Era 3 (2017–2023)"

    def test_era4_lower_bound(self):
        assert get_era(2024) == "Era 4 (2024–present)"

    def test_era4_future_year(self):
        assert get_era(2050) == "Era 4 (2024–present)"

    def test_unknown_before_era1(self):
        assert get_era(1990) == "Unknown"

    def test_era_labels_are_contiguous(self):
        """No year between 1997 and 2023 should fall through to Unknown."""
        for year in range(1997, 2024):
            assert get_era(year) != "Unknown", f"Year {year} returned Unknown"


# ---------------------------------------------------------------------------
# ERA_LABELS sanity checks
# ---------------------------------------------------------------------------


class TestEraLabels:
    def test_all_four_eras_defined(self):
        assert len(ERA_LABELS) == 4

    def test_no_overlap_between_eras(self):
        all_years = []
        for years_range in ERA_LABELS.values():
            all_years.extend(years_range)
        assert len(all_years) == len(set(all_years)), "Eras overlap"


# ---------------------------------------------------------------------------
# api_get
# ---------------------------------------------------------------------------


class TestApiGet:
    def test_successful_request_returns_json(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "ok"}
        with patch("requests.get", return_value=mock_resp):
            result = api_get("/health")
        assert result == {"status": "ok"}

    def test_connection_error_returns_none(self):
        with patch("requests.get", side_effect=ConnectionError("refused")):
            result = api_get("/health")
        assert result is None

    def test_http_error_returns_none(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.side_effect = Exception("500 Server Error")
        with patch("requests.get", return_value=mock_resp):
            result = api_get("/health")
        assert result is None

    def test_timeout_returns_none(self):
        import requests as req
        with patch("requests.get", side_effect=req.exceptions.Timeout()):
            result = api_get("/health")
        assert result is None


# ---------------------------------------------------------------------------
# fetch_bronze_stats
# ---------------------------------------------------------------------------


class TestFetchBronzeStats:
    def test_returns_empty_dataframe_when_no_db(self):
        with patch("dashboard.app.get_conn", return_value=None):
            result = fetch_bronze_stats()
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_returns_dataframe_with_mock_conn(self):
        mock_conn = MagicMock()
        cur = mock_conn.cursor.return_value.__enter__.return_value
        # First cursor block: list tables
        cur.fetchall.return_value = [("raw_1997",), ("raw_2003",)]
        # Second cursor block (loop): row counts
        cur.fetchone.side_effect = [(100,), (200,)]

        with patch("dashboard.app.get_conn", return_value=mock_conn):
            result = fetch_bronze_stats()

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 2
        assert set(result.columns) == {"table", "rows"}

    def test_row_counts_match(self):
        mock_conn = MagicMock()
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [("raw_1997",)]
        cur.fetchone.side_effect = [(365,)]

        with patch("dashboard.app.get_conn", return_value=mock_conn):
            result = fetch_bronze_stats()

        assert result.iloc[0]["rows"] == 365
        assert result.iloc[0]["table"] == "raw_1997"


# ---------------------------------------------------------------------------
# fetch_bronze_sample
# ---------------------------------------------------------------------------


class TestFetchBronzeSample:
    def test_returns_empty_dataframe_when_no_db(self):
        with patch("dashboard.app.get_conn", return_value=None):
            result = fetch_bronze_sample("raw_1997")
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_returns_dataframe_with_mock_conn(self):
        mock_conn = MagicMock()
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.description = [("date",), ("temp",)]
        cur.fetchall.return_value = [("2020-01-01", "25.3"), ("2020-01-02", "24.1")]

        with patch("dashboard.app.get_conn", return_value=mock_conn):
            result = fetch_bronze_sample("raw_1997", limit=2)

        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == ["date", "temp"]
        assert len(result) == 2

    def test_respects_limit_parameter(self):
        mock_conn = MagicMock()
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.description = [("date",)]
        cur.fetchall.return_value = []

        with patch("dashboard.app.get_conn", return_value=mock_conn):
            fetch_bronze_sample("raw_1997", limit=42)

        call_args = cur.execute.call_args
        assert 42 in call_args[0][1]
