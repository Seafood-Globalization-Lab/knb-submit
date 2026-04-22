######### ARTIS model EMLassemblyline workflow ##############
# created: 2026-03-23 by Althea Marks

# This script adapts the automatically generated EMLassemblyline (EAL) workflow.
# Run this script to generate the Ecological Metadata Language (EML) metadata
# documentation that accompanies the ARTIS dataset releases on the KNB data repository.

# Create workflow template ------------------------------------------------

# Creates the directory structure for data package contents and EMLassemblyline.
# Do not rerun - here for documentation only.
# template_directories("./", "metadata-files")

# Required packages (with version control) --------------------------------

# Use pak to set software repo to Posit package manager and pin a
# snapshot date for reproducibility
pak::repo_add(CRAN = "RSPM@2025-10-01")
pak::pak(c(
  "EDIorg/EMLassemblyline",
  "usethis",
  "arrow",
  "dplyr",
  "tools",
  "stringr",
  "glue",
  "readr",
  "EML",
  "config",
  "here",
  "bibtex"
))
{
  library(EMLassemblyline)
  library(usethis)
  library(arrow)
  library(dplyr)
  library(tools)
  library(stringr)
  library(glue)
  library(readr)
  library(EML)
  library(config)
  library(here)
  library(bibtex)
}

# Get config values -------------------------------------------------

cfg <- config::get()

# Derive all filenames from config values
dataset_title <- glue("Aquatic Resource Trade in Species (ARTIS) Database {cfg$model_version} {cfg$prod_type} {cfg$year_start}-{cfg$year_end}")
final_eml_name <- glue("ARTIS_{cfg$model_version}_{cfg$prod_type}_EML.xml")
validation_report_filename <- glue("07-post-processing-validation_{cfg$model_version_sem}_{cfg$prod_type}.html")
validation_report_name <- glue("ARTIS {cfg$model_version} {cfg$prod_type} Data Validation Report")
validation_description <- glue("Self-contained HTML report summarising data validation and assumption checks for the ARTIS {cfg$model_version} {cfg$prod_type} dataset.")

# File Paths -------------------------------------------------------------

# Set path to pre-released local ARTIS dataset in .Renviron file at project level:
# usethis::edit_r_environ(scope = c("project"))

path_artis_files  <- Sys.getenv("ARTIS_DB_PATH")
path_metadata_dir <- "./artis_metadata_files"
path_templates    <- file.path(path_metadata_dir, "metadata_templates")
path_data         <- file.path(path_metadata_dir, "data_objects")
path_eml          <- file.path(path_metadata_dir, "eml")

# Load custom helper functions -------------------------------------------
source("./functions/ARTIS_EAL_helper_functions.R")

# Clean up metadata-files/metadata_templates -----------------------------

# Running EAL template functions will not overwrite existing files in metadata_templates/.
# Delete templated files if rerunning the script to avoid stale attribute/catvars files.

#### THIS DELETES FILES
if (cfg$clean_up_templates == TRUE) {
  file.remove(
    c(list.files(
        path_templates,
        pattern = "^(attributes_|catvars_).*\\.txt$",
        full.names = TRUE
      ),
      list.files(
        path_eml,
        pattern = ".*\\.xml$",
        full.names = TRUE
      )
    )
  )
}

# Read in ARTIS definitions ----------------------------------------------

# Run test script that checks if long-lived ARTIS data dictionaries contain expected values and schema
# to populate EAL tempates.
testthat::test_file(path = "./tests/test_artis_dictionaries_valid.R")

# Read in long-lived ARTIS data dictionaries (attribute definitions).
# Most values will not need updating between release versions unless new columns
# are added or names are changed.
# Open files in a spreadsheet editor (Excel) if edits are needed.
# WARNING: editing and saving in Excel will quietly add non-UTF-8 encoding
# characters. sanitize_encoding() is applied at read-in to clean these up.

artis_defs_attr <- readr::read_tsv(
  file.path(path_metadata_dir, "artis_dictionary_tbl_attributes.txt"),
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

artis_defs_catvars <- readr::read_tsv(
  file.path(path_metadata_dir, "artis_dictionary_tbl_attributes_catvars.txt"),
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

artis_defs_hs_v <- readr::read_tsv(
  file.path(path_metadata_dir, "artis_dictionary_hs_version.txt"),
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

artis_defs_tbl <- readr::read_tsv(
  file.path(path_metadata_dir, "artis_dictionary_tbl.txt"),
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()


# Read in ARTIS citations ------------------------------------------------

# Read .bib file and convert each entry to a bibtex string
bib_entries <- bibtex::read.bib(file.path(path_metadata_dir, "artis_citations.bib"))

# Convert to named list of bibtex strings for EML
citation_list <- lapply(bib_entries, \(entry) {
  list(bibtex = format(entry, style = "bibtex"))
}) %>% unname()

# Setup .csv ARTIS files for EMLassemblyline -----------------------------

if (cfg$convert_parquets == TRUE) {
  # get filepaths for representative ARTIS data files (one trade, one consumption, all reference)
  paths_artis_subset <- get_parquet_data_subset(
    path_data_dir = path_artis_files
  )
  # convert representative parquet files to CSV for EAL template workflow
  # This WILL overwrite existing files if names match
  convert_artis_to_csv(
    paths_artis_parquet = paths_artis_subset,
    path_write_csv      = path_data
  )
}

# Create metadata templates ----------------------------------------------

# Boilerplate EAL template function calls. Remove functions and arguments
# you don't need. Read the docs before editing: ?template_core_metadata

# Create core templates (required for all data packages)
# Will NOT overwrite existing files
EMLassemblyline::template_core_metadata(
  path      = path_templates,
  license   = "CCBY",
  file.type = ".md"
)

# Create table attributes template (required when data tables are present).
# EAL is not compatible with parquet format - supply representative CSVs.
EMLassemblyline::template_table_attributes(
  path       = path_templates,
  data.path  = path_data,
  data.table = list.files(path_data,
  pattern = "\\.csv$")  # add pattern)
)

# Delete custom_units.txt - automatically generated and not required
file.remove(file.path(path_templates, "custom_units.txt"))

# view_unit_dictionary()

# Join ARTIS definitions - attribute templates ---------------------------

# Vector of general ARTIS datatable names (without model version info)
# used to match against versioned "attributes_*.txt" template files
artis_gen_tbl_names <- c(
  "consumption",
  "reference_countries",
  "reference_hs6_taxa_resolution",
  "reference_hs6",
  "reference_production",
  "reference_sciname",
  "reference_trade_baci",
  "trade"
)

purrr::walk(artis_gen_tbl_names, \(a_gen_tbl_name) {

  # match general table name to its versioned attributes_*.txt file
  an_attr_tbl_file <- list.files(
    path    = path_templates,
    pattern = paste0(a_gen_tbl_name, "\\.txt$")
  )

  an_attr_tbl <- read.delim(file.path(path_templates, an_attr_tbl_file))

  # keep attributeName column only — drop auto-generated values
  an_attr_tbl <- an_attr_tbl[1]

  # filter ARTIS attribute definitions to this table and join
  artis_filter_defs <- artis_defs_attr |>
    filter(datatable_general_name == a_gen_tbl_name) |>
    select(-datatable_general_name)

  an_attr_tbl <- an_attr_tbl |>
    left_join(artis_filter_defs, by = "attributeName")

  write.table(
    an_attr_tbl,
    file      = list.files(path_templates, pattern = paste0(a_gen_tbl_name, "\\.txt$"), full.names = TRUE),
    sep       = "\t",
    row.names = FALSE,
    quote     = FALSE
  )
  message(glue("Updated `{an_attr_tbl_file}` with ARTIS attribute definitions"))
})

# Join ARTIS definitions - categorical variable templates ----------------

# Create categorical variables template (required when attributes templates
# contain variables with a "categorical" class)
EMLassemblyline::template_categorical_variables(
  path      = path_templates,
  data.path = path_data
)

purrr::walk(artis_gen_tbl_names, \(a_gen_tbl_name) {

  # match general table name to its versioned catvars_*.txt file
  a_catvars_file <- list.files(
    path    = path_templates,
    pattern = paste0("catvars_.*", a_gen_tbl_name, "\\.txt$")
  )

  # skip if no catvars file found for this table
  if (length(a_catvars_file) == 0) {
    message(glue("No catvars file found for `{a_gen_tbl_name}` - skipping"))
    return(invisible(NULL))
  }

  # filter catvars definitions to this table, drop the general name column
  artis_filter_catvars <- artis_defs_catvars |>
    filter(datatable_general_name == a_gen_tbl_name) |>
    select(-datatable_general_name)

  a_catvar_tbl <- read.delim(file.path(path_templates, a_catvars_file))

  a_catvar_tbl <- a_catvar_tbl |>
    select(-definition) |>
    left_join(artis_filter_catvars, by = c("attributeName", "code"))

  # append hs_version definitions if present in this table's categorical variables
  if (any(a_catvar_tbl$attributeName %in% "hs_version")) {
    a_catvar_tbl <- a_catvar_tbl |>
      bind_rows(artis_defs_hs_v) |>
      # sort so rows with definitions appear before empty/NA ones
      arrange(attributeName, code, is.na(definition), definition == "") |>
      # drop duplicate attributeName x code rows, keeping the one with a definition
      distinct(attributeName, code, .keep_all = TRUE) |>
      mutate(
        definition = case_when(
          # HS92 is filtered out of the ARTIS model but manually added here
          # for reference_hs6 — not maintained in artis_hs_version_dictionary.txt
          attributeName == "hs_version" & code == "HS92" ~ "Harmonized System version 1992",
          .default = definition
        )
      )
  }

  write.table(
    a_catvar_tbl,
    file      = file.path(path_templates, a_catvars_file),
    sep       = "\t",
    row.names = FALSE,
    quote     = FALSE,
    na        = ""
  )
  message(glue("Updated `{a_catvars_file}` with ARTIS catvars definitions"))
})

# Geographic coverage ----------------------------------------------------
# Not relevant for ARTIS global coverage — commented out for reference

# EMLassemblyline::template_geographic_coverage(
#   path = path_templates,
#   data.path = path_data,
#   data.table = "",
#   lat.col = "",
#   lon.col = "",
#   site.col = "")

# Taxonomic coverage -----------------------------------------------------
# Not implemented — too many taxa for current workflow. Retained for reference.

# remotes::install_github("EDIorg/taxonomyCleanr")
# library(taxonomyCleanr)
# taxonomyCleanr::view_taxa_authorities()
# EMLassemblyline::template_taxonomic_coverage(
#   path = path_templates,
#   data.path = path_data,
#   taxa.table = "",
#   taxa.col = "",
#   taxa.name.type = "",
#   taxa.authority = 3)

# ARTIS datatable definitions --------------------------------------------

# Pull ordered vector of table-level descriptions for use in make_eml()
artis_defs_tbl <- artis_defs_tbl |>
  arrange(datatable_general_name) |>
  pull(definition)

# Make EML from metadata templates ---------------------------------------

EMLassemblyline::make_eml(
  path                    = path_templates,
  data.path               = path_data,
  eml.path                = path_eml,
  dataset.title           = dataset_title,
  temporal.coverage       = c(cfg$year_start, cfg$year_end),
  # Not working with calling config value string, throwing warning message. Hard coding as a work around. 
  geographic.description  = "Global coverage",
  geographic.coordinates  = c(cfg$geo_coord_N, cfg$geo_coord_E, cfg$geo_coord_S, cfg$geo_coord_W), 
  maintenance.description = cfg$maintenance_description,
  # exclude html validation file in path_data
  data.table              = list.files(path_data, pattern = "\\.csv$"),
  data.table.name         = tools::file_path_sans_ext(list.files(path_data, pattern = "\\.csv$")),
  data.table.description  = artis_defs_tbl,
  # add validation file
  other.entity            = validation_report_filename,
  other.entity.name       = validation_report_name,
  other.entity.description = validation_description, 
  package.id              = "artis_eml_draft",
  user.domain             = cfg$data_repo
)

# Post EAL EML corrections -----------------------------------------------
# The EAL workflow generates EML describing 8 representative .csv files.
# This section replaces those <dataTable> elements with ones describing
# the actual 148 parquet files that make up the ARTIS dataset on KNB.
# Strategy: clone each of the 8 EAL-generated <dataTable> elements,
# keeping the full <attributeList> and all other metadata intact.
# Only <physical>, <entityName>, and <entityDescription> are replaced
# to reflect each parquet file. Consumption and trade templates are each
# cloned many times (once per parquet file); reference table templates
# are cloned once each.

# Read the EAL-generated EML back in as an R list object
eml <- EML::read_eml(file.path(path_eml, "artis_eml_draft.xml"))

# get all parquet file paths relative to path_artis_files
artis_parquet_files <- list.files(
  path_artis_files, 
  recursive = TRUE,
  pattern = "\\.parquet$"
)

# Extract the 8 EAL-generated template <dataTable> elements from the EML,
# keyed by ARTIS table type name for lookup below.
# End-of-string anchors ($) prevent partial matches
# (e.g. "trade$" does not match "reference_trade_baci").
dataTable_csv <- eml$dataset$dataTable

templates <- list(
  consumption                   = get_template(dataTable_csv, "consumption$"),
  trade                         = get_template(dataTable_csv, "trade$"),
  reference_countries           = get_template(dataTable_csv, "reference_countries$"),
  reference_hs6_taxa_resolution = get_template(dataTable_csv, "reference_hs6_taxa_resolution$"),
  reference_hs6                 = get_template(dataTable_csv, "reference_hs6$"),
  reference_production          = get_template(dataTable_csv, "reference_production$"),
  reference_sciname             = get_template(dataTable_csv, "reference_sciname$"),
  reference_trade_baci          = get_template(dataTable_csv, "reference_trade_baci$")
)

# Generate a cloned <dataTable> element for each of the 148 parquet files.
# Each file is matched to its ARTIS table type, then the corresponding
# template <dataTable> is copied and three fields are overwritten:
# <physical>, <entityName>, and <entityDescription>.
# All other fields including <attributeList> are preserved from the template.
# Files that fail type detection are dropped with a warning.
parquet_datatables <- purrr::map(artis_parquet_files, \(f) {
  table_type <- detect_table_type(f)

  if (is.na(table_type)) {
    warning(glue("Could not detect table type for: {f}. File was dropped from EML."))
    return(NULL)
  }

  make_parquet_datatable(templates[[table_type]], f, path_artis_files)
}) |>
  purrr::discard(is.null)

# Replace the 8 representative CSV <dataTable> elements with the 148
# parquet <dataTable> elements via list assignment.
# The original EAL-generated EML is preserved unchanged at "artis_eml_draft.xml".
eml$dataset$dataTable <- parquet_datatables

# Document the partitioned file structure at the dataset level
eml$dataset$additionalInfo <- cfg$artis_eml_additionalInfo

# Add Literature cited from artis_citations.bib file
eml$dataset$literatureCited <- list(citation = citation_list)

EML::write_eml(eml, file.path(path_eml, final_eml_name))

# Validate the final EML
EML::eml_validate(file.path(path_eml, final_eml_name))
