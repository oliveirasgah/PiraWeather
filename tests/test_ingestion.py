"""Tests for ingestion/ingest.py — pure helpers + write/orchestration paths."""

import re
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from ingestion import ingest as ingest_mod
from ingestion.ingest import (
    _write_table,
    header_row,
    ingest_year,
    load_section,
    make_columns,
    sanitize_name,
)


# ---------------------------------------------------------------------------
# header_row
# ---------------------------------------------------------------------------


class TestHeaderRow:
    def test_era1_lower_bound(self):
        assert header_row(1997) == 11

    def test_era1_upper_bound(self):
        assert header_row(2002) == 11

    def test_era2_lower_bound(self):
        assert header_row(2003) == 14

    def test_era2_upper_bound(self):
        assert header_row(2016) == 14

    def test_era3_lower_bound(self):
        assert header_row(2017) == 6

    def test_era3_upper_bound(self):
        assert header_row(2023) == 6

    def test_era4_lower_bound(self):
        assert header_row(2024) == 2

    def test_era4_future(self):
        assert header_row(2035) == 2


# ---------------------------------------------------------------------------
# sanitize_name
# ---------------------------------------------------------------------------


class TestSanitizeName:
    def test_plain_ascii(self):
        assert sanitize_name("Temperature", 0) == "Temperature"

    def test_spaces_become_underscores(self):
        assert sanitize_name("Wind Speed", 0) == "Wind_Speed"

    def test_special_chars_removed(self):
        # parentheses and degree symbol → underscores, then stripped; ° has no ASCII equivalent
        result = sanitize_name("Temp (°C)", 0)
        assert re.search(r"[^a-zA-Z0-9_]", result) is None

    def test_accented_char_normalized_to_ascii(self):
        assert sanitize_name("Horário", 0) == "Horario"

    def test_multiple_accents_normalized(self):
        assert sanitize_name("Ação", 0) == "Acao"

    def test_cedilla_normalized(self):
        assert sanitize_name("Preço", 0) == "Preco"

    def test_nan_numpy(self):
        assert sanitize_name(np.nan, 3) == "_col_3"

    def test_nan_string(self):
        assert sanitize_name("nan", 5) == "_col_5"

    def test_whitespace_only(self):
        assert sanitize_name("   ", 1) == "_col_1"

    def test_empty_string(self):
        assert sanitize_name("", 2) == "_col_2"

    def test_leading_trailing_specials_stripped(self):
        result = sanitize_name("(column)", 0)
        assert result == "column"

    def test_consecutive_specials_collapsed(self):
        result = sanitize_name("a  b", 0)
        assert result == "a_b"

    def test_index_used_in_fallback(self):
        assert sanitize_name(np.nan, 99) == "_col_99"



# ---------------------------------------------------------------------------
# make_columns
# ---------------------------------------------------------------------------


class TestMakeColumns:
    def test_no_duplicates_unchanged(self):
        assert make_columns(["Date", "Temp", "Humidity"]) == ["Date", "Temp", "Humidity"]

    def test_duplicates_receive_positional_suffix(self):
        result = make_columns(["Date", "Temp", "Temp"])
        assert result[0] == "Date"
        # Both Temp occurrences must be unique
        assert result[1] != result[2]
        assert "Temp" in result[1]
        assert "Temp" in result[2]

    def test_all_same_all_unique(self):
        result = make_columns(["X", "X", "X"])
        assert len(set(result)) == 3

    def test_nan_columns_deduplicated(self):
        result = make_columns([np.nan, np.nan])
        assert len(set(result)) == 2

    def test_single_column(self):
        assert make_columns(["Solo"]) == ["Solo"]

    def test_empty_list(self):
        assert make_columns([]) == []


# ---------------------------------------------------------------------------
# load_section
# ---------------------------------------------------------------------------


def _make_raw(n_rows: int = 20, n_cols: int = 3) -> pd.DataFrame:
    """Minimal 'raw Excel' DataFrame with a header row at index 0."""
    headers = {i: [f"H{i}"] + [f"val_{i}_{r}" for r in range(n_rows - 1)] for i in range(n_cols)}
    return pd.DataFrame(headers)


class TestLoadSection:
    URL = "http://example.com/diario2020.xls"

    def test_metadata_columns_present(self):
        raw = _make_raw()
        result = load_section(raw, header_idx=0, row_start=1, row_end=None, year=2020, url=self.URL)
        assert "_source_year" in result.columns
        assert "_source_url" in result.columns
        assert "_ingested_at" in result.columns

    def test_source_year_value(self):
        raw = _make_raw()
        result = load_section(raw, header_idx=0, row_start=1, row_end=None, year=2020, url=self.URL)
        assert (result["_source_year"] == "2020").all()

    def test_source_url_value(self):
        raw = _make_raw()
        result = load_section(raw, header_idx=0, row_start=1, row_end=None, year=2020, url=self.URL)
        assert (result["_source_url"] == self.URL).all()

    def test_row_slicing_with_end(self):
        raw = _make_raw(n_rows=20)
        # header at row 0, data rows 1–5 (row_end exclusive)
        result = load_section(raw, header_idx=0, row_start=1, row_end=6, year=2020, url=self.URL)
        assert len(result) == 5

    def test_row_slicing_no_end(self):
        raw = _make_raw(n_rows=10)
        result = load_section(raw, header_idx=0, row_start=1, row_end=None, year=2020, url=self.URL)
        assert len(result) == 9  # all rows after header

    def test_data_columns_cast_to_string(self):
        raw = _make_raw()
        result = load_section(raw, header_idx=0, row_start=1, row_end=None, year=2020, url=self.URL)
        data_cols = [c for c in result.columns if not c.startswith("_")]
        for col in data_cols:
            assert pd.api.types.is_string_dtype(result[col])

    def test_index_reset(self):
        raw = _make_raw(n_rows=10)
        result = load_section(raw, header_idx=0, row_start=5, row_end=None, year=2020, url=self.URL)
        assert list(result.index) == list(range(len(result)))

    def test_column_names_from_header_row(self):
        raw = _make_raw(n_cols=3)
        # header row 0 has values H0, H1, H2
        result = load_section(raw, header_idx=0, row_start=1, row_end=None, year=2020, url=self.URL)
        data_cols = [c for c in result.columns if not c.startswith("_")]
        assert data_cols == ["H0", "H1", "H2"]


# ---------------------------------------------------------------------------
# sanitize_name — security edge cases (SQL metacharacters in headers)
# ---------------------------------------------------------------------------


class TestSanitizeNameSecurity:
    """Headers come from XLS files we don't author — assume hostile input."""

    @pytest.mark.parametrize(
        "header",
        [
            'Tar"; DROP TABLE x',
            "col; DROP TABLE foo;",
            "a' OR 1=1 --",
            'col"--',
            "x`y",
            "name\nwith\nnewlines",
            "x\x00y",
        ],
    )
    def test_sql_metacharacters_stripped(self, header):
        result = sanitize_name(header, 0)
        # Only [A-Za-z0-9_] survive after sanitization.
        assert re.fullmatch(r"[A-Za-z0-9_]+", result), result

    def test_make_columns_neutralizes_injection_headers(self):
        result = make_columns(['Tar"; DROP TABLE x', "Tar"])
        for name in result:
            assert re.fullmatch(r"[A-Za-z0-9_]+", name), name
        assert len(set(result)) == 2  # duplicates after sanitization get suffixed


# ---------------------------------------------------------------------------
# _write_table — DB-write path (mocked cursor)
# ---------------------------------------------------------------------------


class TestWriteTable:
    def _df(self):
        return pd.DataFrame({"Tar": ["1.0", "2.0"], "UR": ["50", "60"]})

    def test_drops_creates_then_copies(self):
        cur = MagicMock()
        _write_table(cur, "raw_2020", self._df())
        executed = [call.args[0] for call in cur.execute.call_args_list]
        assert any("DROP TABLE IF EXISTS bronze.raw_2020" in s for s in executed)
        assert any("CREATE TABLE bronze.raw_2020" in s for s in executed)
        assert cur.copy_expert.called
        copy_sql = cur.copy_expert.call_args.args[0]
        assert "COPY bronze.raw_2020 FROM STDIN" in copy_sql

    def test_columns_quoted_in_create(self):
        cur = MagicMock()
        df = pd.DataFrame({"Tar_AVG": ["1"], "UR_inst": ["2"]})
        _write_table(cur, "raw_2020", df)
        create_sql = next(
            call.args[0]
            for call in cur.execute.call_args_list
            if "CREATE TABLE" in call.args[0]
        )
        assert '"Tar_AVG" TEXT' in create_sql
        assert '"UR_inst" TEXT' in create_sql

    def test_uses_columnar_storage(self):
        cur = MagicMock()
        _write_table(cur, "raw_2020", self._df())
        create_sql = next(
            call.args[0]
            for call in cur.execute.call_args_list
            if "CREATE TABLE" in call.args[0]
        )
        assert "USING columnar" in create_sql


# ---------------------------------------------------------------------------
# ingest_year — orchestration paths (read_excel + DB mocked)
# ---------------------------------------------------------------------------


def _mock_conn(existing_tables=()):
    """Build a connection mock whose cursor returns the given existing tables."""
    con = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = [(t,) for t in existing_tables]
    con.cursor.return_value.__enter__.return_value = cur
    con.cursor.return_value.__exit__.return_value = False
    return con, cur


class TestIngestYear:
    def test_skips_when_table_exists_and_not_force(self):
        con, cur = _mock_conn(existing_tables=["raw_2020"])
        # 2020 is in the past on any realistic CI clock, so the
        # `year < current_year` skip-branch fires without time-mocking.
        with patch.object(ingest_mod.pd, "read_excel") as m_read:
            ingest_year(con, year=2020, force=False)
        m_read.assert_not_called()
        assert all("DROP" not in c.args[0] for c in cur.execute.call_args_list)

    def test_force_reingests_existing_table(self):
        con, cur = _mock_conn(existing_tables=["raw_2020"])
        df = pd.DataFrame({0: ["H0"] + [f"v{i}" for i in range(20)]})
        # header_row(2020)=14 → row_start=17, but our toy frame is small;
        # load_section will accept a smaller slice and produce 0+ rows.
        with patch.object(ingest_mod.pd, "read_excel", return_value=df) as m_read:
            ingest_year(con, year=2020, force=True)
        m_read.assert_called_once()

    def test_swallows_read_excel_failure_without_db_writes(self):
        con, _ = _mock_conn()
        with patch.object(ingest_mod.pd, "read_excel", side_effect=RuntimeError("boom")):
            ingest_year(con, year=2020, force=True)
        con.commit.assert_not_called()
        con.rollback.assert_not_called()

    def test_rolls_back_on_db_error(self):
        con, cur = _mock_conn()
        # First execute (CREATE EXTENSION/TABLE) raises
        cur.execute.side_effect = RuntimeError("db down")
        df = pd.DataFrame({"H0": ["v1", "v2", "v3", "v4", "v5"]})
        with patch.object(ingest_mod.pd, "read_excel", return_value=df):
            ingest_year(con, year=2020, force=True)
        con.rollback.assert_called_once()
        con.commit.assert_not_called()

    def test_uses_source_url_template(self):
        con, _ = _mock_conn()
        with patch.object(ingest_mod.pd, "read_excel", side_effect=RuntimeError("stop")) as m_read:
            ingest_year(con, year=2020, force=True)
        called_url = m_read.call_args.args[0]
        assert "diario2020.xls" in called_url


class TestIngestLoop:
    def test_continues_across_years_when_single_year_read_fails(self):
        """The fault-tolerance contract is in ingest_year (not the outer loop):
        a failed read_excel must not stop subsequent years from being attempted."""
        years_seen = []

        def read_side_effect(url):
            year = int(re.search(r"diario(\d{4})", url).group(1))
            years_seen.append(year)
            if year == 2020:
                raise RuntimeError("simulated network failure")
            return pd.DataFrame({"H0": ["v1", "v2", "v3", "v4", "v5"]})

        con = MagicMock()
        # cursor.fetchall returns no existing tables → not skipped
        con.cursor.return_value.__enter__.return_value.fetchall.return_value = []

        with patch.object(ingest_mod, "_get_conn", return_value=con), \
             patch.object(ingest_mod.pd, "read_excel", side_effect=read_side_effect):
            ingest_mod.ingest(years=[2019, 2020, 2021])
        assert years_seen == [2019, 2020, 2021]
