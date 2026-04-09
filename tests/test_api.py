"""Tests for api/main.py — validation helpers, DB helpers, and HTTP endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from api.main import _fetch_rows, _get_conn, _list_tables, _valid_identifier, app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_conn():
    """Psycopg2 connection double with sensible defaults."""
    conn = MagicMock()
    cur = conn.cursor.return_value.__enter__.return_value
    cur.fetchall.return_value = []
    cur.fetchone.return_value = (1,)
    cur.description = []
    return conn


@pytest.fixture
def client(mock_conn):
    """TestClient with DB dependency overridden and _pool patched non-None."""
    app.dependency_overrides[_get_conn] = lambda: mock_conn
    with patch("api.main._pool", MagicMock()):
        with TestClient(app) as c:
            yield c
    app.dependency_overrides.clear()


# ---------------------------------------------------------------------------
# _valid_identifier
# ---------------------------------------------------------------------------


class TestValidIdentifier:
    def test_simple_name(self):
        assert _valid_identifier("table_name") == "table_name"

    def test_name_with_numbers(self):
        assert _valid_identifier("table_123") == "table_123"

    def test_underscore_prefix(self):
        assert _valid_identifier("_private") == "_private"

    def test_mixed_case(self):
        assert _valid_identifier("MyTable") == "MyTable"

    def test_rejects_space(self):
        with pytest.raises(HTTPException) as exc_info:
            _valid_identifier("bad name")
        assert exc_info.value.status_code == 400

    def test_rejects_sql_injection(self):
        with pytest.raises(HTTPException):
            _valid_identifier("t; DROP TABLE users--")

    def test_rejects_empty_string(self):
        with pytest.raises(HTTPException):
            _valid_identifier("")

    def test_rejects_leading_digit(self):
        with pytest.raises(HTTPException):
            _valid_identifier("1table")

    def test_rejects_hyphen(self):
        with pytest.raises(HTTPException):
            _valid_identifier("my-table")

    def test_rejects_dot(self):
        with pytest.raises(HTTPException):
            _valid_identifier("schema.table")


# ---------------------------------------------------------------------------
# _list_tables
# ---------------------------------------------------------------------------


class TestListTables:
    def test_returns_table_names(self, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [("table_a",), ("table_b",)]
        assert _list_tables(mock_conn, "bronze") == ["table_a", "table_b"]

    def test_empty_schema_returns_empty_list(self, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        assert _list_tables(mock_conn, "gold") == []

    def test_schema_passed_as_query_param(self, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        _list_tables(mock_conn, "silver")
        call_args = cur.execute.call_args
        # Schema name must appear in the positional params tuple, not inline SQL
        assert "silver" in call_args[0][1]


# ---------------------------------------------------------------------------
# _fetch_rows
# ---------------------------------------------------------------------------


class TestFetchRows:
    def test_raises_404_when_table_missing(self, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = None  # table doesn't exist
        with pytest.raises(HTTPException) as exc_info:
            _fetch_rows(mock_conn, "silver", "missing_table", limit=10, offset=0)
        assert exc_info.value.status_code == 404

    def test_returns_columns_rows_count(self, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1,)  # table exists
        cur.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        cur.description = [("id",), ("name",)]

        result = _fetch_rows(mock_conn, "silver", "users", limit=10, offset=0)

        assert result["columns"] == ["id", "name"]
        assert result["count"] == 2
        assert result["rows"] == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

    def test_empty_table_returns_zero_count(self, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1,)
        cur.fetchall.return_value = []
        cur.description = [("id",)]

        result = _fetch_rows(mock_conn, "gold", "empty_table", limit=10, offset=0)
        assert result["count"] == 0
        assert result["rows"] == []


# ---------------------------------------------------------------------------
# HTTP endpoints
# ---------------------------------------------------------------------------


class TestHealthEndpoint:
    def test_ok_when_pool_exists(self):
        with patch("api.main._pool", MagicMock()):
            with TestClient(app) as c:
                resp = c.get("/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["db_connected"] is True

    def test_db_connected_false_when_no_pool(self):
        with patch("api.main._pool", None):
            with TestClient(app) as c:
                resp = c.get("/health")
        assert resp.status_code == 200
        assert resp.json()["db_connected"] is False


class TestBronzeEndpoints:
    def test_years_empty(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        resp = client.get("/bronze/years")
        assert resp.status_code == 200
        assert resp.json()["years"] == []

    def test_years_parsed_and_deduplicated(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        # raw_2016_s1 and raw_2016_s2 both map to year 2016
        cur.fetchall.return_value = [
            ("raw_1997",), ("raw_2003",), ("raw_2016_s1",), ("raw_2016_s2",)
        ]
        resp = client.get("/bronze/years")
        assert resp.status_code == 200
        years = resp.json()["years"]
        assert "1997" in years
        assert "2003" in years
        assert "2016" in years
        assert len(years) == len(set(years))  # no duplicates

    def test_stats_returns_table_list(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [("raw_1997",)]
        cur.fetchone.return_value = (365,)
        resp = client.get("/bronze/stats")
        assert resp.status_code == 200
        assert "tables" in resp.json()


class TestSilverGoldEndpoints:
    def test_silver_tables_list(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = [("daily_weather",)]
        resp = client.get("/silver/tables")
        assert resp.status_code == 200
        assert "tables" in resp.json()

    def test_gold_tables_list(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchall.return_value = []
        resp = client.get("/gold/tables")
        assert resp.status_code == 200
        assert resp.json()["tables"] == []

    def test_silver_table_invalid_identifier_rejected(self, client):
        resp = client.get("/silver/bad-table")
        assert resp.status_code == 400

    def test_gold_table_invalid_identifier_rejected(self, client):
        resp = client.get("/gold/1invalid")
        assert resp.status_code == 400

    def test_silver_table_not_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = None  # table doesn't exist
        resp = client.get("/silver/nonexistent_table")
        assert resp.status_code == 404

    def test_gold_table_not_found(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = None
        resp = client.get("/gold/nonexistent_table")
        assert resp.status_code == 404

    def test_silver_table_data_returned(self, client, mock_conn):
        cur = mock_conn.cursor.return_value.__enter__.return_value
        cur.fetchone.return_value = (1,)
        cur.fetchall.return_value = [(42, "rain")]
        cur.description = [("id",), ("condition",)]
        resp = client.get("/silver/valid_table")
        assert resp.status_code == 200
        body = resp.json()
        assert body["columns"] == ["id", "condition"]
        assert body["count"] == 1


class TestNoDatabaseAvailable:
    def test_returns_503_when_pool_is_none(self):
        # No dependency override — _get_conn will raise 503 because _pool is None
        with patch("api.main._pool", None):
            with TestClient(app, raise_server_exceptions=False) as c:
                resp = c.get("/bronze/years")
        assert resp.status_code == 503
