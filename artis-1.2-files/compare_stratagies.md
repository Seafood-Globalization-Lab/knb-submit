# Partitioning Strategy: Custom Layout vs. Single Parquet

## Directory Structure

The custom partitioned layout organises files by HS version and year:

```
outputs_combined/
├── HS96/
│   ├── ARTIS_v1.2.0_SAU_consumption_HS96_1996.parquet
│   └── ...
├── HS02/
├── HS07/
├── HS12/
└── HS17/
```

---

## Comparison

| | Single parquet | Custom partitioned layout |
|---|---|---|
| **Human readability** | ❌ One opaque blob | ✅ Directory structure communicates HS version and year instantly |
| **Discoverability on data repo** | ❌ Users must download everything to explore | ✅ Users can download only the HS version / year they need |
| **Arrow `open_dataset()` query** | ✅ Simple, no config needed | ⚠️ Requires `hive_style = FALSE` + `partitioning` schema |
| **Partition pruning** | N/A | ✅ Arrow skips irrelevant HS subdirs when filtering by `hs_version` |
| **Row group pruning** | ✅ Full benefit within one file | ⚠️ Only within each per-year file — less effective since files are already small |
| **Compression efficiency** | ✅ Better — more repeated values across a larger file | ⚠️ Slightly worse — 66 small files compress less efficiently |
| **Metadata overhead** | ✅ One footer to read | ⚠️ Arrow must read 66 footers to build the dataset schema |
| **File count on repo** | ✅ One file to manage | ⚠️ 66 files — more to document, version, and maintain |

---

## Opening the Dataset in R

Because the layout is not hive-style (`key=value/`), users must specify the
partitioning schema explicitly:

```r
library(arrow)

cnsmp_ds <- arrow::open_dataset(
  "path/to/outputs_combined",
  format       = "parquet",
  partitioning = arrow::schema(hs_version = arrow::utf8()),
  hive_style   = FALSE
)
```

### Example queries

Filter by HS version and year:

```r
cnsmp_ds |>
  dplyr::filter(hs_version == "HS02", year == 2010) |>
  dplyr::collect()
```

Count rows per HS version for a given year (confirms cross-partition reads):

```r
cnsmp_ds |>
  dplyr::filter(year == 2012) |>
  dplyr::count(hs_version) |>
  dplyr::collect()
```

---

## Key Recommendation

Include the `open_dataset()` snippet above in your repository README so users
do not have to discover the `hive_style = FALSE` argument themselves.