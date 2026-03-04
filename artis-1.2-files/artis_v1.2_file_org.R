# test parquet dataset reads with custom partitioning and format reference tables
# 2026-02-27
# Althea Marks

# Self-contained script to convert partitioned consumption .qs2 files into
# individual parquet files organised by HS version:
#   {base_outdir}/{HS_version}/ARTIS_v1.2.0_SAU_consumption_{HS_version}_{year}.parquet

# Substancial organizational changes to output files in v2.0. However for this release 
# we wanted to update the final organization and column name changes now instead of waiting to release the v2.0 or v3.0 data


# Libraries -------------------------------------------------------------------
pak::repo_add(CRAN = "RSPM@2025-10-31")
#pak::repo_get()

pkgs <- c(
  "qs2",
  "arrow",
  "glue",
  "foreach",
  "doParallel",
  "dplyr",
  "data.table",
  "magrittr",
  "janitor"
)

pak::pkg_install(pkgs)
invisible(lapply(pkgs, library, character.only = TRUE))

# Config - edit these paths ---------------------------------------------------
ARTIS_file <- "consumption"
artis_version      <- "v1.2"
prod_data_type     <- "FAO"
estimate_data_type <- "midpoint"
output_dir <- "~/Documents/UW-SAFS/ARTIS/data/outputs_1.2.0_FAO_2025-11-20"
search_dir  <- file.path(output_dir, "snet")
base_outdir <- glue("{output_dir}/KNB/{ARTIS_file}")
ref_dir <- file.path(output_dir, "KNB_reference_tables_raw")
ref_outdir <- file.path(output_dir, "KNB", "reference_tables")

#single_file <- "~/Documents/UW-SAFS/ARTIS/data/outputs_1.2.0_SAU_2025-11-14/KNB/datasets/ARTIS_v1.2.0_consumption_SAU_mid_all_HS_yrs_2025-12-01.parquet"
single_file <- "~/Documents/UW-SAFS/ARTIS/data/outputs_2.0_FAO_2025-09-11/outputs_combined/ARTIS_v2.0_trade_FAO_mid_all_HS_yrs_2025-09-12.parquet"


# Convert ARTIS Files to Parquet ----------------------------------------

## Input file pattern and regex ------------------------------------------------
# Input file pattern: {date}_consumption_midpoint_{year}_{HS_version}.qs2
# e.g. "2025-11-17_consumption_midpoint_2002_HS02.qs2"
# Unfortunately trade is different :( ARTIS_v1.2.0_FAO_trade_HS96_2001.parquet

if (ARTIS_file == "trade") {
  # Legacy trade filename: 2025-11-20_S-net_raw_midpoint_HS12_2014.qs2
  # search_pattern matches the fixed middle portion
  search_pattern <- glue("S-net_raw_{estimate_data_type}")
  # Capture groups: [2] = HS version, [3] = year  <-- note reversed order vs consumption
  fname_regex    <- glue("^\\d{{4}}-\\d{{2}}-\\d{{2}}_S-net_raw_{estimate_data_type}_(HS\\d{{2}})_(\\d{{4}})\\.qs2$")
} else if (ARTIS_file == "consumption") {
  # Consumption filename: 2025-11-17_consumption_midpoint_2002_HS02.qs2
  search_pattern <- glue("{ARTIS_file}_{estimate_data_type}")
  # Capture groups: [2] = year, [3] = HS version
  fname_regex    <- glue("^\\d{{4}}-\\d{{2}}-\\d{{2}}_{ARTIS_file}_{estimate_data_type}_(\\d{{4}})_(HS\\d{{2}})\\.qs2$")
}

## Write model output file to "partitioned" parquet dataset --------------------
all_files <- list.files(
  path       = search_dir,
  pattern    = search_pattern,
  recursive  = TRUE,
  full.names = TRUE
)

if (length(all_files) == 0) stop("No matching files found in: ", search_dir)

message(glue("Found {length(all_files)} file(s) to process.\n"))

## Set up parallel cluster -----------------------------------------------------
n_cores <- min(length(all_files), parallel::detectCores() - 1)
cl <- makeCluster(n_cores)
registerDoParallel(cl)
on.exit(stopCluster(cl), add = TRUE)

message(glue("Using {n_cores} cores.\n"))

## Write partitioned parquet files in parallel ---------------------------------
foreach(
  f = all_files,
  i = seq_along(all_files),
  .packages = c("qs2", "arrow", "glue", "magrittr", "dplyr"),
  .export   = c("fname_regex", "base_outdir", "artis_version",
                "prod_data_type", "ARTIS_file", "all_files")
) %dopar% {

  fname <- basename(f)

  parts <- regmatches(fname, regexec(fname_regex, fname))[[1]]

  # Parse captures — trade has (HS_version, year); consumption has (year, HS_version)
  if (ARTIS_file == "trade") {
    hs_version <- parts[2]
    year_str   <- parts[3]
  } else {
    year_str   <- parts[2]
    hs_version <- parts[3]
  }

  hs_outdir <- file.path(base_outdir, hs_version)
  dir.create(hs_outdir, recursive = TRUE, showWarnings = FALSE)

  # Uniform output naming: ARTIS_v1.2.0_FAO_trade_HS96_2001.parquet
  out_file <- file.path(
    hs_outdir,
    glue("ARTIS_{artis_version}_{prod_data_type}_{ARTIS_file}_{hs_version}_{year_str}.parquet")
  )

  chunk <- qs2::qd_read(f)

  # reorder columns and update names 
  if (ARTIS_file == "trade") {
    chunk <- chunk %>%
      select(
        hs_version,
        year,
        prod_country_iso3c = source_country_iso3c, # rename
        exporter_iso3c,
        importer_iso3c,
        export_type = dom_source, # rename
        hs6,
        sciname,
        habitat,
        method,
        product_weight_t,
        live_weight_t
      )
  } else if (ARTIS_file == "consumption"){
    chunk <- chunk %>% 
      select(
        hs_version,
        year,
        prod_country_iso3c = source_country_iso3c,
        exporter_iso3c,
        consumer_iso3c,
        consumption_type = consumption_source,
        sciname,
        sciname_hs_modified,
        habitat,
        method,
        end_use,
        consumption_live_t,
        consumption_live_t_capped,
        consumption_percap_live_kg,
        consumption_percap_live_kg_capped
      )
  }
  # Order data by two most commonly filtered columns - for parquet efficiency 
  chunk <- chunk %>% 
    arrange(hs_version, year)

  arrow::write_parquet(chunk, out_file)
  rm(chunk); gc()

  message(glue("[{i}/{length(all_files)}] Written:  {basename(out_file)}\n"))
}



# Reorganize Reference Tables --------------------------------------------
# ref_dir
# ref_outdir

# create directory if it does not exits
if (!dir.exists(ref_outdir)) {
  dir.create(ref_outdir)
}

ref_file_pat <- glue("ARTIS_{artis_version}_{prod_data_type}_reference")

## sciname --------------------------------------------

# read in
sciname <- fread(file.path(ref_dir, "sciname_metadata.csv"), data.table = FALSE) %>% 
  janitor::clean_names() %>% 
  arrange(sciname)

fwrite(sciname, file.path(ref_outdir, glue("{ref_file_pat}_sciname.csv")))
arrow::write_parquet(sciname, file.path(ref_outdir, glue("{ref_file_pat}_sciname.parquet")))


## baci --------------------------------------------

baci <- fread(
  file.path(ref_dir, "baci.csv"), 
  colClasses = list(character = "hs6"),
  data.table = FALSE
) %>% 
  janitor::clean_names() %>% 
  select(
    hs_version,
    year,
    exporter_iso3c,
    importer_iso3c,
    hs6,
    product_weight_t
  ) %>% 
  arrange(hs_version, year)
  
fwrite(baci, file.path(ref_outdir, glue("{ref_file_pat}_baci_trade.csv")))
arrow::write_parquet(baci, file.path(ref_outdir, glue("{ref_file_pat}_baci_trade.parquet")))

## code_max_resolved --------------------------------------------

code_max <- fread(
  file.path(ref_dir, "code_max_resolved.csv"), 
  # preserve leading zeros
  colClasses = list(character = "hs6"),
  data.table = FALSE
) %>% 
  janitor::clean_names() %>% 
  mutate(hs_version = sub("^HS", "", hs_version)) %>% 
  select(
    hs_version,
    sciname,
    sciname_hs6_modified = sciname_hs_modified,
    match_category,
    hs6_description = description, 
    modification,
    hs6_clade = hs_clade,
    hs6_taxa_level = code_taxa_level,
    prod_taxa_level,
    hs6_taxa_level_numeric = code_taxa_level_numeric  
  ) %>% 
  arrange(hs_version, sciname)

fwrite(code_max, file.path(ref_outdir, glue("{ref_file_pat}_hs6_taxa_resolution.csv")))
arrow::write_parquet(code_max, file.path(ref_outdir, glue("{ref_file_pat}_hs6_taxa_resolution.parquet")))


## prod --------------------------------------------

prod <- fread(
  file.path(ref_dir, "prod.csv"), 
  data.table = FALSE
) %>% 
  janitor::clean_names() %>% 
  select(
    year,
    prod_country_iso3c = iso3c,
    sciname,
    method,
    habitat,
    live_weight_t
  ) %>% 
  arrange(year, prod_country_iso3c)

fwrite(prod, file.path(ref_outdir, glue("{ref_file_pat}_production.csv")))
arrow::write_parquet(prod, file.path(ref_outdir, glue("{ref_file_pat}_production.parquet")))


## countries --------------------------------------------

countries <- fread(
  file.path(ref_dir, "countries.csv"), 
  data.table = FALSE
) %>% 
  janitor::clean_names() %>% 
  select(
    country_iso3c = iso3c,
    country_name,
    owid_region,
    continent
  ) %>% 
  arrange(country_iso3c)

fwrite(countries, file.path(ref_outdir, glue("{ref_file_pat}_countries.csv")))
arrow::write_parquet(countries, file.path(ref_outdir, glue("{ref_file_pat}_countries.parquet")))


## products --------------------------------------------

products <- fread(
  file.path(ref_dir, "products.csv"), 
  # preserve leading zeros
  colClasses = list(character = "hs6", character = "parent"),
  data.table = FALSE
) %>% 
  janitor::clean_names() %>% 
  select(
    comtrade_classification = classification,
    hs6,
    hs6_description = description,
    hs6_parent = parent,
    hs6_presentation = presentation,
    hs6_state = state
  ) %>% 
  arrange(comtrade_classification, hs6)

fwrite(products, file.path(ref_outdir, glue("{ref_file_pat}_hs6.csv")))
arrow::write_parquet(products, file.path(ref_outdir, glue("{ref_file_pat}_hs6.parquet")))












# test parquet dataset ----------------------------------------

## test what open_dataset() arguements are required ---------------------

# Full function test
#  cnsmp_ds <- arrow::open_dataset(
#    base_outdir,
#    format = "parquet",
#    partitioning = arrow::schema(hs_version = arrow::utf8()),
#    hive_style = FALSE
#  )

# test removing arguements
cnsmp_ds <- arrow::open_dataset(
  base_outdir#,
  #partitioning = arrow::schema(hs_version = arrow::utf8())
)

tmp_1 <- cnsmp_ds |>
  dplyr::filter(
    hs_version == "HS02", 
    year == 2010,
    consumer_iso3c == "USA",
    sciname == "crassostrea virginica") |>
  dplyr::collect()

### Testing note
# in arrow::open_dataset() call, additional arguements can be removed and tmp_1 still successfully filtered and read into memory. 
# Arguements `format` and `hive_style` are not required, function can infer. 
# Arguement `partiitioning` still collects same data, but probably in a different method:
# with the partitioning arguement the function is parsing from the directory names, without the function is reading from column
# inside the parquet file. Doesn't have noticable performance impacts with this example, but might with larger data chunks 


## Test time of "partitioning" arguement ----------------------------------

### With partitioning argument --------------------------------------------------
# Arrow parses hs_version from directory names — enables partition pruning
time_with <- system.time({
  cnsmp_ds_with <- arrow::open_dataset(
    base_outdir,
    partitioning = arrow::schema(hs_version = arrow::utf8())
  )
  
  result_with <- cnsmp_ds_with |>
    dplyr::filter(
      hs_version == "HS02",
      year == 2010,
      consumer_iso3c == "USA"
    ) |>
    dplyr::collect()
})

### Without partitioning argument -----------------------------------------------
# Arrow reads hs_version from column inside each parquet file — no dir pruning
time_without <- system.time({
  cnsmp_ds_without <- arrow::open_dataset(base_outdir)
  
  result_without <- cnsmp_ds_without |>
    dplyr::filter(
      hs_version == "HS02",
      year == 2010,
      consumer_iso3c == "USA"
    ) |>
    dplyr::collect()
})

### Results ---------------------------------------------------------------------
# Confirm both queries return identical row counts
stopifnot(nrow(result_with) == nrow(result_without))

data.frame(
  approach      = c("with partitioning", "without partitioning"),
  elapsed_secs  = c(time_with["elapsed"], time_without["elapsed"]),
  rows_returned = c(nrow(result_with), nrow(result_without))
)

#              approach elapsed_secs rows_returned
#1    with partitioning        0.433        144803
#2 without partitioning        0.229        144803

 
## Test time of "partitioning" arguement - with bigger data chunk ---------------------------------------------

time_with <- system.time({
  cnsmp_ds_with <- arrow::open_dataset(
    base_outdir,
    partitioning = arrow::schema(hs_version = arrow::utf8())
  )
  
  result_with <- cnsmp_ds_with |>
    dplyr::filter(
      hs_version == "HS02",
      year %in% 2004:2020,
      consumer_iso3c == "USA"
    ) |>
    dplyr::collect()
})

time_without <- system.time({
  cnsmp_ds_without <- arrow::open_dataset(base_outdir)
  
  result_without <- cnsmp_ds_without |>
    dplyr::filter(
      hs_version == "HS02",
      year %in% 2004:2020,
      consumer_iso3c == "USA"
    ) |>
    dplyr::collect()
})

stopifnot(nrow(result_with) == nrow(result_without))

data.frame(
  approach      = c("with partitioning", "without partitioning"),
  elapsed_secs  = c(time_with["elapsed"], time_without["elapsed"]),
  rows_returned = c(nrow(result_with), nrow(result_without))
)

#              approach elapsed_secs rows_returned
#1    with partitioning        4.363       2385847
#2 without partitioning        3.850       2385847


## open_dataset() arguement conclusion ------------------------------------

# (Claude AI) Practical conclusion for your use case: the partitioning argument gives no meaningful 
# performance benefit here, and you could omit it. The more important factor would be 
# if you had hundreds of HS subdirectories or much larger files. I'd recommend dropping 
# it from your recommended open_dataset() snippet to keep it simpler for end users of the data repository.


# Validate that partioned dataset  ------------------------------------

## Open datasets ---------------------------------------------------------------
partitioned_ds <- arrow::open_dataset(base_outdir)
single_ds      <- arrow::open_dataset(single_file)

# Get all partitioned parquet files
all_files <- list.files(base_outdir, pattern = "\\.parquet$", recursive = TRUE, full.names = TRUE)

# Row counts from metadata footer only — no data scan
n_partitioned <- sum(sapply(all_files, function(f) {
  arrow::ParquetFileReader$create(f)$num_rows
}))

n_single <- arrow::ParquetFileReader$create(single_file)$num_rows

# Comparison
data.frame(
  source       = c("partitioned", "single"),
  n_files      = c(length(all_files), 1),
  n_rows       = c(n_partitioned, n_single),
  rows_match   = c(n_partitioned == n_single, n_partitioned == n_single)
)
#        source n_files    n_rows rows_match
#1 partitioned      66 457294979       TRUE
#2      single       1 457294979       TRUE

# 2. Column names and types ---------------------------------------------------
schema_partitioned <- partitioned_ds |> schema()
schema_single      <- single_ds |> schema()

# Compare field-level names and types only — ignore file-level metadata
fields_match <- sapply(names(schema_partitioned), function(col) {
  schema_partitioned$GetFieldByName(col)$type == schema_single$GetFieldByName(col)$type
})

data.frame(
  column      = names(fields_match),
  types_match = fields_match
)
#                                                              column types_match
# year                                                           year        TRUE
# hs_version                                               hs_version        TRUE
# source_country_iso3c                           source_country_iso3c        TRUE
# exporter_iso3c                                       exporter_iso3c        TRUE
# consumer_iso3c                                       consumer_iso3c        TRUE
# consumption_source                               consumption_source        TRUE
# sciname                                                     sciname        TRUE
# sciname_hs_modified                             sciname_hs_modified        TRUE
# habitat                                                     habitat        TRUE
# method                                                       method        TRUE
# end_use                                                     end_use        TRUE
# consumption_live_t                               consumption_live_t        TRUE
# consumption_live_t_capped                 consumption_live_t_capped        TRUE
# consumption_percap_live_kg               consumption_percap_live_kg        TRUE
# consumption_percap_live_kg_capped consumption_percap_live_kg_capped        TRUE

# 3. Row counts per hs_version x year -----------------------------------------
# Confirms data is split correctly across files with no rows lost or duplicated
counts_partitioned <- partitioned_ds |>
  count(hs_version, year) |>
  collect() |>
  arrange(hs_version, year)

counts_single <- single_ds |>
  count(hs_version, year) |>
  collect() |>
  arrange(hs_version, year)

cat("Row counts per hs_version x year match:", isTRUE(all.equal(counts_partitioned, counts_single)), "\n\n")

# 4. Distinct values per key categorical column --------------------------------
# Confirms no categories were dropped or corrupted during the write
cat_cols <- c("hs_version", "consumer_iso3c", "source_country_iso3c",
              "habitat", "method", "end_use", "consumption_source")

distinct_match <- sapply(cat_cols, function(col) {
  vals_p <- partitioned_ds |> distinct(across(all_of(col))) |> collect() |> pull()
  vals_s <- single_ds      |> distinct(across(all_of(col))) |> collect() |> pull()
  setequal(vals_p, vals_s)
})

print(data.frame(column = names(distinct_match), distinct_values_match = distinct_match))


# Column order -----------------------------------------------------------

# Consumption suggestion
    # hs_version,
    # year,
    # source_country_iso3c,
    # exporter_iso3c,
    # consumer_iso3c,
    # consumption_source,
    # sciname,
    # sciname_hs_modified,
    # habitat,
    # method,
    # end_use,
    # consumption_live_t,
    # consumption_live_t_capped,
    # consumption_percap_live_kg,
    # consumption_percap_live_kg_capped



# 2.0 conventions --------------------------------------------------------

ds_2.0 <- open_dataset("~/Documents/UW-SAFS/ARTIS/data/outputs_2.1.1_SAU_2025-10-28/outputs_combined/ARTIS_2.1.1_trade_SAU_mid_all_HS_yrs_2025-10-31.parquet")
