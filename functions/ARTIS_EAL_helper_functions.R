# ARTIS EML Assembly Line Helper Functions
# These functions support the EMLassemblyline workflow for generating
# and post-processing EML metadata for the ARTIS dataset.

# Read-in helpers --------------------------------------------------------

# Helper: sanitize Windows-1252 encoding artifacts introduced by Excel.
# EMLassemblyline recommends editing template .txt files in a spreadsheet
# editor, but Excel silently inserts non-UTF-8 characters (e.g. non-breaking
# spaces \xca, en-dashes \xd0) on save. This function coerces all character
# columns back to valid UTF-8 after reading.
sanitize_encoding <- function(df) {
  df |> dplyr::mutate(
    dplyr::across(where(is.character), \(x) iconv(x, from = "Windows-1252", to = "UTF-8"))
  )
}

# Arrow schema helpers ---------------------------------------------------

# Helper: map Arrow type strings to EML class strings.
# Used when building EML attribute templates from parquet file schemas.
help_arrow_to_eml_class <- function(type_str) {
  dplyr::case_when(
    type_str %in% c("string", "utf8", "large_utf8")                       ~ "categorical",
    type_str %in% c("int32", "int64", "uint32", "uint64", "int16", "int8") ~ "numeric",
    type_str %in% c("double", "float", "float32", "float64")               ~ "numeric",
    type_str %in% c("date32", "date64")                                    ~ "Date",
    type_str %in% c("timestamp[ms]", "timestamp[us]", "timestamp[s]")      ~ "Date",
    .default = "character"
  )
}

# Helper: build and write an EML attribute template from an Arrow schema.
# Reads the schema of a parquet-backed Arrow table and writes a tab-delimited
# attributes .txt file formatted for EMLassemblyline.
help_write_eml_attributes <- function(data_table, out_path) {
  field_names <- arrow::schema(data_table)$names
  field_types <- sapply(
    seq_along(field_names),
    \(i) arrow::schema(data_table)[[i]]$type$ToString()
  )
  eml_classes <- help_arrow_to_eml_class(field_types)

  # follow template structure provided by EMLassemblyline
  attr_df <- tibble::tibble(
    attributeName               = field_names,
    attributeDefinition         = "",
    class                       = eml_classes,
    unit                        = dplyr::if_else(eml_classes == "numeric", "!Add units here!", ""),
    dateTimeFormatString        = dplyr::if_else(eml_classes == "Date", "!Add datetime specifier here!", ""),
    missingValueCode            = "",
    missingValueCodeExplanation = ""
  )

  write.table(
    attr_df,
    file      = out_path,
    sep       = "\t",
    row.names = FALSE,
    quote     = FALSE,
    na        = ""
  )
  message("Written: ", out_path)
}

# CSV conversion helpers -------------------------------------------------

# Helper: collect a representative subset of ARTIS parquet file paths.
# Returns all reference table parquet paths plus one representative trade
# and one representative consumption file (both from HS96) for use in the
# EMLassemblyline CSV-based template workflow.
get_parquet_data_subset <- function(path_data_dir) {
  paths_parquet <- list.files(
    file.path(path_data_dir, "reference_tables"),
    full.names = TRUE
  ) |>
    c(
      # only want single representative trade file for metadata
      list.files(
        file.path(path_data_dir, "trade", "HS96"),
        full.names = TRUE
      )[1]
    ) |>
    c(
      # only want single representative consumption file for metadata
      list.files(
        file.path(path_data_dir, "consumption", "HS96"),
        full.names = TRUE
      )[1]
    )
  return(paths_parquet)
}

# Helper: convert a vector of parquet file paths to CSVs written to path_write_csv.
# Trade and consumption filenames are generalized by stripping the HS version
# and year suffix so EMLassemblyline treats them as single representative tables.
# This WILL overwrite existing files if names match.
convert_artis_to_csv <- function(paths_artis_parquet, path_write_csv) {
  purrr::walk(paths_artis_parquet, \(a_file_path) {
    a_table     <- arrow::read_parquet(a_file_path)
    a_file_name <- tools::file_path_sans_ext(basename(a_file_path))

    # strip HS version and year from trade and consumption filenames
    if (stringr::str_detect(a_file_name, "trade|consumption")) {
      a_file_name <- stringr::str_replace(a_file_name, "_HS\\d+_\\d+", "")
    }

    arrow::write_csv_arrow(a_table, file.path(path_write_csv, paste0(a_file_name, ".csv")))
  })
}

# Post-EAL EML correction helpers ----------------------------------------
# These functions support replacing the 8 EAL-generated CSV <dataTable>
# elements with <dataTable> elements describing the full 148 parquet files.

# Helper: build a <physical> element describing a parquet file.
# EML <physical> expects objectName, size, and dataFormat at minimum.
# Parquet is a binary columnar format - use externallyDefinedFormat
# rather than EML's built-in text format descriptors.
make_parquet_physical <- function(parquet_file, base_path) {
  full_path  <- file.path(base_path, parquet_file)
  size_bytes <- file.info(full_path)$size

  list(
    objectName = basename(parquet_file),
    size       = list(size = as.character(size_bytes), unit = "bytes"),
    dataFormat = list(
      externallyDefinedFormat = list(formatName = "Apache Parquet")
    )
  )
}

# Helper: detect which of the 8 ARTIS table types a parquet file belongs to.
# This determines which EAL-generated <dataTable> template to clone.
# Note: reference_hs6_taxa_resolution is checked before reference_hs6
# to prevent the shorter pattern from matching both.
detect_table_type <- function(parquet_file) {
  dplyr::case_when(
    grepl("^consumption/",                 parquet_file) ~ "consumption",
    grepl("^trade/",                       parquet_file) ~ "trade",
    grepl("reference_countries",           parquet_file) ~ "reference_countries",
    grepl("reference_hs6_taxa_resolution", parquet_file) ~ "reference_hs6_taxa_resolution",
    grepl("reference_hs6\\.parquet",       parquet_file) ~ "reference_hs6",
    grepl("reference_production",          parquet_file) ~ "reference_production",
    grepl("reference_sciname",             parquet_file) ~ "reference_sciname",
    grepl("reference_trade_baci",          parquet_file) ~ "reference_trade_baci",
    .default = NA_character_
  )
}

# Helper: build an <entityDescription> string for a parquet file.
# For consumption and trade files, appends the HS version and year
# extracted from the filename (e.g. "HS02", "2002") to the base
# template description to indicate this file is a subset of the full dataset.
# Reference table files have no HS version or year in their filenames
# and receive the base template description unchanged.
make_entity_description <- function(template_dt, parquet_file) {
  base_desc  <- template_dt$entityDescription
  hs_version <- stringr::str_extract(parquet_file, "HS\\d{2}")
  year       <- stringr::str_extract(parquet_file, "\\d{4}(?=\\.parquet)")

  if (is.na(hs_version) && is.na(year)) {
    base_desc
  } else {
    glue::glue(
      "{base_desc} (This file contains a portion of the dataset ",
      "HS version: {hs_version}, Year: {year})"
    )
  }
}

# Helper: clone a template <dataTable> for a given parquet file.
# The entire <dataTable> R list object is copied from the EAL-generated template,
# preserving <attributeList> and all other metadata unchanged.
# Only <physical>, <entityName>, and <entityDescription> are overwritten
# to reflect the specific parquet file being described.
make_parquet_datatable <- function(template_dt, parquet_file, base_path) {
  dt                   <- template_dt
  dt$physical          <- make_parquet_physical(parquet_file, base_path)
  dt$entityName        <- basename(parquet_file)
  dt$entityDescription <- make_entity_description(template_dt, parquet_file)
  dt
}

# Helper: extract a single <dataTable> element from an EML dataTable list
# by matching a regex pattern against <entityName>.
# Accepts orig_datatables explicitly to avoid relying on the calling environment.
get_template <- function(orig_datatables, name_pattern) {
  idx <- which(sapply(orig_datatables, \(dt) grepl(name_pattern, dt$entityName)))
  orig_datatables[[idx]]
}