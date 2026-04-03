# Column Mapping to Latest Schema (2017–present)

## Era Overview

| Era | Years | Header row index | Notes |
|---|---|---|---|
| Era 1a | 1997–1999 | 11 | Reduced column set; `Horario` without accent |
| Era 1b | 2000–2002 | 11 | Full era 1 columns; `Horário` with accent |
| Era 2  | 2003–2016 | 14 | |
| Era 3  | 2017–2023 | 6  | |
| Era 3  | 2024–present | 2 | Same columns as 2017–2023 |

Era 3 columns are identical across 2017–present — only the header row position differs.

---

## Era 1a: 1997–1999 (header row 11)

| Old column | New column | Notes |
|---|---|---|
| `Dia Juliano` | reconstruct `TIMESTAMP` | Julian day |
| `Horario` | reconstruct `TIMESTAMP` | HHMM → minutes: `x - int(x/100)*40` |
| `Tar` | `Tar_AVG` | |
| `UR` | `UR_inst` | Appears before Vento in this era |
| `Vento` *(1st)* | `Vvento_ms_AVG` | Speed in m/s |
| `Vento` *(2nd)* | `Dvento_G` | Direction in degrees |
| `Rad.Solar` | `Qg_AVG` | |
| `PAR` | `PAR_AVG` | |
| `Rad.Liq.` | `Rn_Avg` | |
| `Niv Tanq`, `Chuva` | `Chuva_mm` / *(drop)* | Only `Chuva` maps forward |

---

## Era 1b: 2000–2002 (header row 11)

| Old column | New column | Notes |
|---|---|---|
| `Dia Juliano` | reconstruct `TIMESTAMP` | Julian day |
| `Horário` | reconstruct `TIMESTAMP` | HHMM → minutes: `x - int(x/100)*40` |
| `Tar` | `Tar_AVG` | |
| `Vento` *(1st)* | `Vvento_ms_AVG` | Speed in m/s |
| `Vento` *(2nd)* | `Dvento_G` | Direction in degrees |
| `Rad.Solar` | `Qg_AVG` | |
| `PAR` | `PAR_AVG` | |
| `Rad.Liq.` | `Rn_Avg` | |
| `UR` | `UR_inst` | Appears after Vento in this era |
| `Chuva` | `Chuva_mm` | |
| `RS EPP` (2000) / `Eppley` (2001–2002), ` es`, ` ea`, `Tu`, `To`, `  DPV`, `Niv Tanq`, `F.C.S.` | *(drop)* | No equivalent in new schema |

---

## Era 2: 2003–2016 (header row 14)

| Old column | New column | Notes |
|---|---|---|
| `Dia` | reconstruct `TIMESTAMP` | Julian day |
| `Horas` | reconstruct `TIMESTAMP` | HHMM → minutes: `x - int(x/100)*40` |
| `Tar` | `Tar_AVG` | |
| `Vento` | `Vvento_ms_AVG` | |
| `Dir Ven` | `Dvento_G` | |
| `Desv. Pad.` | `Dvento_SD1_WVT` | Wind direction std deviation |
| `Rad. Solar` | `Qg_AVG` | |
| `PAR` | `PAR_AVG` | |
| `Rad.Liq.` | `Rn_Avg` | |
| `UR` | `UR_inst` | |
| `Precip` | `Chuva_mm` | |
| `F.C.S.`, ` es`, ` ea`, `Tu`, `To`, `  DPV`, `Niv Tanq`, `Rad Epp` | *(drop)* | No equivalent in new schema |

---

## Era 3: 2017–present (header rows 6 or 2)

No renaming needed — column names are already the target schema.

---

## Columns only available in Era 3 (NaN for all older years)

`BattV_Avg`, `Patm_kPa_AVG`, `rQg_AVG`, `Qatm_AVG`, `Qsup_AVG`, `Boc_AVG`, `Bol_AVG`,
`Albedo_Avg`, `QatmC_AVG`, `QsupC_AVG`, `Vvento_ms_S_WVT`, `Dvento_D1_WVT`, `PainelT`, `RECORD`
