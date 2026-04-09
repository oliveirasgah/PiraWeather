"""Tests for ingestion/ingest.py — pure helper functions."""

import re
import numpy as np
import pandas as pd


from ingestion.ingest import header_row, load_section, make_columns, sanitize_name


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
