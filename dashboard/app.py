"""
PiraWeather Dashboard

Tab 1 — Bronze Explorer: browse raw ingested data directly from PostgreSQL.
Tab 2 — Analytics: silver/gold layer data served via the API (empty until
         dbt models are implemented).
"""

import pandas as pd
import plotly.express as px
import psycopg2
import requests
import streamlit as st
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_user: str
    postgres_password: str
    postgres_db: str = "piraweather"
    api_url: str = "http://localhost:8000"


settings = Settings()

# ── Configuration ─────────────────────────────────────────────────────────────

CONN_PARAMS = dict(
    host=settings.postgres_host,
    port=settings.postgres_port,
    user=settings.postgres_user,
    password=settings.postgres_password,
    dbname=settings.postgres_db,
)
API_URL = settings.api_url

ERA_LABELS = {
    "Era 1 (1997–2002)": range(1997, 2003),
    "Era 2 (2003–2016)": range(2003, 2017),
    "Era 3 (2017–2023)": range(2017, 2024),
    "Era 4 (2024–present)": range(2024, 9999),
}


def get_era(year: int) -> str:
    for label, years in ERA_LABELS.items():
        if year in years:
            return label
    return "Unknown"


# ── Database helpers ──────────────────────────────────────────────────────────

@st.cache_resource
def get_conn():
    try:
        return psycopg2.connect(**CONN_PARAMS)
    except Exception as e:
        return None


@st.cache_data(ttl=300)
def fetch_bronze_stats() -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'bronze' ORDER BY table_name"
        )
        tables = [row[0] for row in cur.fetchall()]

    rows = []
    with conn.cursor() as cur:
        for table in tables:
            cur.execute(f'SELECT COUNT(*) FROM bronze."{table}"')
            rows.append({"table": table, "rows": cur.fetchone()[0]})
    return pd.DataFrame(rows)


@st.cache_data(ttl=300)
def fetch_bronze_sample(table: str, limit: int = 500) -> pd.DataFrame:
    conn = get_conn()
    if conn is None:
        return pd.DataFrame()
    with conn.cursor() as cur:
        cur.execute(f'SELECT * FROM bronze."{table}" LIMIT %s', (limit,))
        cols = [desc[0] for desc in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


# ── API helpers ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def api_get(path: str) -> dict | None:
    try:
        r = requests.get(f"{API_URL}{path}", timeout=5)
        r.raise_for_status()
        return r.json()
    except Exception:
        return None


# ── App layout ────────────────────────────────────────────────────────────────

st.set_page_config(page_title="PiraWeather", page_icon="🌦️", layout="wide")

with st.sidebar:
    st.title("PiraWeather")
    st.caption("ESALQ/USP Meteorological Station · Piracicaba, SP")
    st.divider()
    if st.button("Refresh data"):
        st.cache_data.clear()
        st.rerun()

conn = get_conn()
if conn is None:
    st.error("Cannot connect to PostgreSQL. Check that the database is running and env vars are set.")
    st.stop()

tab_bronze, tab_analytics = st.tabs(["Bronze Explorer", "Analytics"])

# ── Tab 1: Bronze Explorer ────────────────────────────────────────────────────

with tab_bronze:
    st.header("Bronze Layer — Raw Ingested Data")

    stats_df = fetch_bronze_stats()

    if stats_df.empty:
        st.info("No bronze data yet. Run the pipeline to ingest data.")
    else:
        # Overview metrics
        total_rows = int(stats_df["rows"].sum())
        years = sorted(
            {t.split("_")[1] for t in stats_df["table"] if t.startswith("raw_")}
        )
        col1, col2, col3 = st.columns(3)
        col1.metric("Years loaded", len(years))
        col2.metric("Total rows", f"{total_rows:,}")
        col3.metric("Date range", f"{years[0]} – {years[-1]}" if years else "—")

        st.divider()

        # Row count bar chart
        chart_df = stats_df.copy()
        chart_df["year"] = chart_df["table"].str.extract(r"raw_(\d{4})")
        fig = px.bar(
            chart_df.dropna(subset=["year"]).sort_values("year"),
            x="year",
            y="rows",
            title="Rows per year (bronze tables)",
            labels={"year": "Year", "rows": "Row count"},
        )
        fig.update_layout(showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

        st.divider()

        # Year selector + raw data table
        st.subheader("Explore a year")
        all_tables = stats_df["table"].tolist()
        selected_table = st.selectbox("Table", all_tables)

        if selected_table:
            year_str = selected_table.split("_")[1]
            try:
                era = get_era(int(year_str))
            except ValueError:
                era = "Unknown"
            st.caption(f"Schema era: **{era}**")

            sample = fetch_bronze_sample(selected_table)
            st.dataframe(sample, use_container_width=True)
            st.caption(f"Showing up to 500 rows · {len(sample.columns)} columns")

# ── Tab 2: Analytics ──────────────────────────────────────────────────────────

with tab_analytics:
    st.header("Analytics — Silver & Gold Layers")

    health = api_get("/health")
    if health is None or not health.get("db_connected"):
        st.warning("API is not available. Make sure the api service is running.")
    else:
        silver = api_get("/silver/tables") or {}
        gold = api_get("/gold/tables") or {}

        silver_tables = silver.get("tables", [])
        gold_tables = gold.get("tables", [])

        if not silver_tables and not gold_tables:
            st.info(
                "No silver or gold tables yet. "
                "Implement the dbt models to populate this tab."
            )
        else:
            col_s, col_g = st.columns(2)

            with col_s:
                st.subheader("Silver tables")
                if silver_tables:
                    selected_silver = st.selectbox("Silver table", silver_tables)
                    if selected_silver:
                        data = api_get(f"/silver/{selected_silver}?limit=200")
                        if data:
                            st.dataframe(
                                pd.DataFrame(data["rows"], columns=data["columns"]),
                                use_container_width=True,
                            )
                else:
                    st.caption("No silver tables.")

            with col_g:
                st.subheader("Gold tables")
                if gold_tables:
                    selected_gold = st.selectbox("Gold table", gold_tables)
                    if selected_gold:
                        data = api_get(f"/gold/{selected_gold}?limit=200")
                        if data:
                            st.dataframe(
                                pd.DataFrame(data["rows"], columns=data["columns"]),
                                use_container_width=True,
                            )
                else:
                    st.caption("No gold tables.")
