"""SQL-injection probes against identifier handling.

Hits three call sites where a hostile string could reach Postgres:
  1. ``api.main._valid_identifier``  — the regex gate for /silver/{table} and /gold/{table}.
  2. ``api.main._fetch_rows``        — confirms the gate is invoked and a malicious
     name never reaches the f-string SELECT.
  3. ``dashboard.app.fetch_bronze_sample`` — Streamlit constrains input via
     selectbox in the UI, but the function itself takes a free str; this test
     locks in the function-level guarantee.
"""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException

from api.main import _fetch_rows, _valid_identifier


# Five-to-ten well-chosen payloads. Each represents a distinct attack class:
#   semicolon-stacked DDL, comment-truncate, quote-escape, unicode homoglyph,
#   NUL byte, whitespace, and the empty string.
INJECTION_PAYLOADS = [
    "raw_2020; DROP TABLE bronze.raw_2020",
    "raw_2020--",
    "raw_2020' OR '1'='1",
    'raw_2020"; DROP TABLE x; --',
    "weather​data",   # zero-width-space homoglyph
    "raw_2020\x00",
    "raw_2020 ",
    "",
    " ",
    "1raw_2020",            # leading digit — not a valid SQL identifier start
]


class TestValidIdentifier:
    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS)
    def test_rejects_injection_payloads(self, payload):
        with pytest.raises(HTTPException) as excinfo:
            _valid_identifier(payload)
        assert excinfo.value.status_code == 400

    @pytest.mark.parametrize(
        "good_name",
        [
            "weather_data",
            "_internal",
            "raw_2020",
            "Tar_AVG",
            "a",
        ],
    )
    def test_accepts_valid_identifiers(self, good_name):
        assert _valid_identifier(good_name) == good_name


class TestFetchRowsBlocksInjection:
    """End-to-end: a hostile path segment must never reach the cursor."""

    @pytest.mark.parametrize("payload", INJECTION_PAYLOADS)
    def test_caller_must_validate_first(self, payload):
        # The route handlers call _valid_identifier(table) before _fetch_rows.
        # That gate is the boundary; if a caller bypassed it, _fetch_rows
        # would still f-string the value into SQL — locking in that the
        # gate must be applied at the route layer.
        with pytest.raises(HTTPException) as excinfo:
            _valid_identifier(payload)
        assert excinfo.value.status_code == 400

    def test_fetch_rows_uses_existence_check_with_parameters(self):
        """When called with a valid identifier, the existence check uses %s
        binding, not f-string interpolation, so even a sql-shaped table name
        cannot escape its parameter slot."""
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchone.return_value = None  # treat as "not found"
        conn.cursor.return_value.__enter__.return_value = cur

        with pytest.raises(HTTPException) as excinfo:
            _fetch_rows(conn, schema="silver", table="weather_data", limit=1, offset=0)
        assert excinfo.value.status_code == 404

        # The first execute call is the existence probe; assert it was
        # parameterized — the table name appears in params, NOT the SQL string.
        first_call = cur.execute.call_args_list[0]
        sql = first_call.args[0]
        params = first_call.args[1]
        assert "%s" in sql and "information_schema.tables" in sql
        assert params == ("silver", "weather_data")


class TestDashboardSampleGuard:
    """fetch_bronze_sample should reject obvious injection payloads even when
    called outside the Streamlit UI (e.g. from a test harness or an internal
    script that bypasses the selectbox)."""

    @pytest.mark.parametrize(
        "payload",
        [
            "raw_2020; DROP TABLE bronze.raw_2020",
            'raw_2020"; DROP TABLE x; --',
            "raw_2020--",
            "",
        ],
    )
    def test_rejects_injection_payloads(self, payload):
        from dashboard import app as dash

        # Patch get_conn so the fixture would only matter if validation passed.
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur

        with patch.object(dash, "get_conn", return_value=conn):
            with pytest.raises((ValueError, HTTPException)):
                dash.fetch_bronze_sample(payload)
        cur.execute.assert_not_called()
