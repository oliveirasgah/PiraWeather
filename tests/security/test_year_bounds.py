"""Year-bounds defensive validation in ingest_year.

The function builds both the source URL and the bronze table name from the
``year`` argument; an attacker-controlled non-int value would inject into
``f"raw_{year}"``, which is then interpolated into DDL/COPY in
``_write_table``. argparse blocks bad values from the CLI; these tests lock
in the in-process defense.
"""

from unittest.mock import MagicMock, patch

import pytest

from ingestion import ingest as ingest_mod
from ingestion.ingest import ingest_year


@pytest.fixture
def stub_conn():
    return MagicMock()


@pytest.fixture
def fail_if_fetched():
    """A read_excel patch that fails the test if it ever gets called."""
    with patch.object(ingest_mod.pd, "read_excel") as m:
        m.side_effect = AssertionError("read_excel reached with invalid year")
        yield m


@pytest.mark.parametrize(
    "bad_year",
    [
        -1,
        0,
        1996,                                 # below FIRST_YEAR
        9999,                                 # absurd future
        ingest_mod.date.today().year + 5,     # 5 years out — future fetch we shouldn't allow
    ],
)
def test_out_of_range_int_rejected_before_fetch(bad_year, stub_conn, fail_if_fetched):
    with pytest.raises(ValueError, match="outside"):
        ingest_year(stub_conn, year=bad_year, force=True)
    fail_if_fetched.assert_not_called()


@pytest.mark.parametrize(
    "bad_year",
    [
        "2020",                               # str — even a parseable one
        2020.0,                               # float
        "2020; DROP TABLE bronze.raw_2020",   # injection attempt
        True,                                 # bool subclass of int — must still be rejected
        None,
        ["2020"],
    ],
)
def test_non_int_year_rejected_before_fetch(bad_year, stub_conn, fail_if_fetched):
    with pytest.raises(ValueError, match="must be an int"):
        ingest_year(stub_conn, year=bad_year, force=True)
    fail_if_fetched.assert_not_called()


def test_in_range_year_allowed():
    """Sanity: a valid year passes the guard (and proceeds to fetch, which we stub)."""
    con = MagicMock()
    con.cursor.return_value.__enter__.return_value.fetchall.return_value = []
    with patch.object(
        ingest_mod.pd,
        "read_excel",
        side_effect=RuntimeError("stop after fetch"),  # we don't care past this point
    ) as m:
        ingest_year(con, year=2020, force=True)
    m.assert_called_once()
