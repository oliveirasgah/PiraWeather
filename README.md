# PiraWeather

A project to structure, organize, and explore data available from ESALQ
about weather in Piracicaba-SP.

The data used is available [here](http://www.leb.esalq.usp.br/leb/automatica/pagina6.html).

## Variables
| Variable | Description |
|---|---|
| **TIMESTAMP** | Date/time of measurement |
| **BattV_Avg** | Battery voltage average (power supply monitoring) |
| **Tar_AVG** | Air temperature average (°C) |
| **UR_inst** | Relative humidity instantaneous (%) |
| **Vvento_ms_AVG** | Wind velocity average (m/s) |
| **Dvento_G** | Wind direction (degrees) |
| **Qg_AVG** | Global solar radiation average (W/m² or MJ/m²/day) NrelScienceDirect |
| **PAR_AVG** | Photosynthetically Active Radiation average (µmol/m²/s) |
| **Rn_Avg** | Net radiation average - downward and upward fluxes of shortwave and longwave radiation Net Radiation - an overview - ScienceDirect Topics (W/m²) |
| **Chuva_mm** | Rainfall (mm) |
| **Patm_kPa_AVG** | Atmospheric pressure average (kPa) |
| **rQg_AVG** | Reflected global radiation average (W/m²) |
| **Qatm_AVG** | Atmospheric longwave radiation downward average (W/m²) |
| **Qsup_AVG** | Surface longwave radiation upward average (W/m²) |
| **Boc_AVG** | Bowen ratio or thermal sensor reading average |
| **Bol_AVG** | Another thermal/radiation sensor average |
| **Albedo_Avg** | Surface albedo average - fraction of sunlight reflected by surface (0-1 scale) Albedo - Wikipedia |
| **QatmC_AVG** | Corrected atmospheric radiation average (W/m²) |
| **QsupC_AVG** | Corrected surface radiation average (W/m²) |
| **Vvento_ms_S_WVT** | Wind velocity scalar average with vector calculation |
| **Dvento_D1_WVT** | Wind direction unit vector component |
| **Dvento_SD1_WVT** | Wind direction standard deviation component |
| **PainelT** | Panel temperature (°C) - likely data logger housing temperature |

## Testing

Five layers, each catching a different class of regression. All run locally
against the docker-compose stack; no CI is wired up in this repository (see
"Production Considerations").

| Layer | What it covers |
|---|---|
| L1 — Python unit | `tests/test_*.py` — pure helpers, mocked DB writes, ingestion orchestration |
| L2 — dbt schema tests | `models/*/schema.yml` — physical-bounds ranges, relationships, composite uniqueness, percentile ordering. Uses `dbt-utils`. |
| L3 — dbt singular tests | `tests/dbt_singular/*.sql` — cross-row invariants (GDD monotonicity, drought z-score ↔ category alignment, extreme-events rank completeness, silver partition coverage) |
| L4 — Integration smoke | `tests/integration/` — `@pytest.mark.integration`. End-to-end ingest of a fixture XLS into a real Citus columnar Postgres, served back through FastAPI. |
| L5 — Security | `tests/security/` — SQL-injection probe matrix against `_valid_identifier`, year-bounds rejection in `ingest_year`, `bandit` static scan |

Run the full suite locally:

```bash
pip install -r requirements-test.txt
docker compose up -d postgres                                    # bring up Citus

pytest                                                           # L1 + L5 (unit + security)
pytest -m integration                                            # L4 (integration smoke)

dbt deps                                                         # one-time: install dbt-utils
dbt build --target dev                                           # L2 + L3 (dbt schema + singular)

bandit -r ingestion api dashboard -ll                            # L5 (static scan)
```

The `pytest` default config excludes `-m integration`, so the unit and
security tests stay fast and infra-free; the integration smoke is run
separately when Postgres is up.

## Production Considerations

The following gaps are intentional out-of-scope items for a portfolio
project. Each would be a real prerequisite before this stack handled
non-public traffic:

- **Postgres bound to `0.0.0.0:5432`** — convenient for local dashboards;
  production should bind to a private network and put Postgres behind a
  bastion or VPN.
- **No auth on FastAPI / Streamlit** — endpoints are open. Production
  needs an auth layer (JWT, OIDC, or a reverse-proxy gate).
- **Containers run as `root`** — none of the Dockerfiles add a `USER`
  directive. Production images should run as a dedicated unprivileged user.
- **ESALQ fetch is over HTTP** — the upstream data source doesn't offer
  TLS, so MITM is theoretically possible. Production would mitigate with a
  checksum/signature on the downloaded payload.
- **Single Postgres role** — bronze writer, silver reader, and API
  consumer all share one superuser. Production should split into
  `bronze_writer`, `silver_reader`, and `api_user` with least-privilege
  GRANTs.
- **`.env` committed with dev creds** — fine for a local-only setup;
  rotate and `.gitignore` for any deployment.

These show up as findings in `bandit` only at the function-call level
(B608 etc.); the architectural items above are tracked in this section
because they cannot be asserted by an automated test.
