######### ARTIS model EMLassemblyline workflow ##############
# created: 2026-03-23 by Althea Marks

# This script adapts the automatically generated EMLassemblyline (EAL) workflow
# Run this script to generate the Ecological Metadata Language (EML) metadata
# documentation that accompanies the ARTIS dataset releases on the KNB data repository. 

# Create worflow template ------------------------------------------------

# create directory structure for data package contents and EMLassemblyline
# created the template of this script. Do not rerun, here for documentation. 
# template_directories("./", "metadata-files")

# Required packages (with version control) --------------------------------------------------------

# Update EMLassemblyline and load
# use pak to set software repo to Posit package manager and pin a 
# snapshot date for reproducabilty 
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
  "EML"
))
library(EMLassemblyline)
library(usethis)
library(arrow)
library(dplyr)
library(tools)
library(stringr)
library(glue)
library(readr)
library(EML)

# Personal script config -------------------------------------------------

clean_up_templates <- "yes"
convert_parquets <- "no"

# File Paths -------------------------------------------------------------

# set path to pre-released local ARTIS dataset in .Renviron file at project level: 
# usethis::edit_r_environ(scope = c("project"))

# set latest ARTIS dataset file path
artis_files_path <- Sys.getenv("ARTIS_DB_PATH")

# Define paths for your metadata templates, data, and EML
path_metadata_dir <- "./metadata-files"
# where the EAL .txt file templates will go
path_templates <- file.path(path_metadata_dir, "metadata_templates")
# where ARTIS .csv data subset will be writen to
path_data <- file.path(path_metadata_dir, "data_objects")
# where eml will be written
path_eml <- file.path(path_metadata_dir, "eml")


# Clean up metadata-files/metadata_templates -----------------------------

# running EAL template functions will not overwrite existing files in metadata_templates/
# But it might error out when artis_dictionary tables are joined to attributes_*.txt and 
# catvars_*.txt files. Delete templated files if rerunning script. 

#### THIS DELETES FILES
if (clean_up_templates == "yes") {
  file.remove(
    list.files(
      path_templates,
      pattern = "^(attributes_|catvars_).*\\.txt$",
      full.names = TRUE
    )
  )
}

# Read in ARTIS definitions ----------------------------------------------
# read in long-lived ARTIS data dictionaries (attribute definitions)
# Most values will not need updating between release versions unless new columns 
# are added or names are changed.
# Open file in a spreadsheet (excel) if edits are needed. 
# WARNING editing and saving in from excel will quitely add Non UTF-8 enocing characters 
# and mess up the EML validataion. 

# Clean up excel artifacts upon reading in ARTIS dictionaries
sanitize_encoding <- function(df) {
  df |> mutate(across(where(is.character), \(x) iconv(x, from = "Windows-1252", to = "UTF-8")))
}

artis_defs_attr <- readr::read_tsv(
  "metadata-files/artis_dictionary_tbl_attributes.txt",
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

artis_defs_catvars <- readr::read_tsv(
  "metadata-files/artis_dictionary_tbl_attributes_catvars.txt",
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

artis_defs_hs_v <- readr::read_tsv(
  "metadata-files/artis_dictionary_hs_version.txt",
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

artis_defs_tbl <- readr::read_tsv(
  "metadata-files/artis_dictionary_tbl.txt",
  show_col_types = FALSE,
  na = "NA"
) |> sanitize_encoding()

# Load custom helper functions ----------------------------------------------
source("./functions/ARTIS_EAL_helper_functions.R")

# Setup .csv ARTIS files for EMLassemblyline ------------------------

if(convert_parquets == "yes"){
  # get the filepaths to select representative ARTIS data files
  paths_artis_subset <- get_parquet_data_subset(
    path_data_dir = artis_files_path
  )
  # take artis parquet file paths and convert to csv writen in this metadata repo/directory
  # This WILL write over existing files if the names are the same
  convert_artis_to_csv(
    paths_artis_parquet = paths_artis_subset, 
    path_write_csv = path_data)
}

# Create metadata templates ---------------------------------------------------

# Below is a list of boiler plate function calls for creating metadata templates.
# They are meant to be a reminder and save you a little time. Remove the 
# functions and arguments you don't need AND ... don't forget to read the docs! 
# E.g. ?template_core_metadata

# Create core templates (required for all data packages)
# will NOT OVERWRITE existing files

EMLassemblyline::template_core_metadata(
  path = path_templates,
  license = "CCBY",
  file.type = ".md")

# Create table attributes template (required when data tables are present)
# function is not compatible with parquet file format. 
# supply vector of names if multiple files
# will not overwrite existing files

EMLassemblyline::template_table_attributes(
  path = path_templates,
  data.path = path_data,
  data.table = list.files(path_data)
)

# Delete custom_units.txt - automatically generated and not required or used
file.remove("metadata-files/metadata_templates/custom_units.txt")

# view standard unit descriptions
#view_unit_dictionary()

# Join ARTIS data defitions - attribute temlplates -----------------------------------------------

# vector of general ARTIS datatable names (without model version info) to use across model releases
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

# match general names to version specific "attributes_*.txt" files to join 
# on artis_data_dictionary_attributes.txt values

purrr::walk(artis_gen_tbl_names, \(a_gen_tbl_name){
  
  # match to the specific attribute file with the general tbl name
  an_attr_tbl_file <- list.files(
    path = path_templates,
    pattern = paste0(a_gen_tbl_name, "\\.txt$")
  )

  # read in matched file
  an_attr_tbl <- read.delim(file.path(path_templates, an_attr_tbl_file))

  # remove automatically generated values - keep attributeName column
  an_attr_tbl <- an_attr_tbl[1]

  # join ARTIS attribute definitions onto matched attribute file.
  # remove datatable_general_name column before join.
  artis_filter_defs <- artis_defs_attr %>% 
    filter(datatable_general_name == a_gen_tbl_name) %>% 
    select(-c(datatable_general_name))

  an_attr_tbl <- an_attr_tbl %>% 
    left_join(
      artis_filter_defs,
      by = c("attributeName"))
  
  # write out updated attribute .txt file
  write.table(
    an_attr_tbl,
    file = list.files(
      path = path_templates,
      pattern = paste0(a_gen_tbl_name, "\\.txt$"),
      full.names = TRUE),
    sep = "\t",
    row.names = FALSE,
    quote = FALSE
  )
  message(glue("Updated `{an_attr_tbl_file}` with ARTIS attribute definitions"))
}
) # end of purrr::walk()


# Join ARTIS data defitions - attribute temlplates --------------------------------------------------
# 2026-03-25 AM - Can I selectively skip definiting iso3c values - and just define 
# custom ARTIS categories?

# Create categorical variables template (required when attributes templates
# contains variables with a "categorical" class)

EMLassemblyline::template_categorical_variables(
  path = path_templates, 
  data.path = file.path(path_data)
)

# join ARTIS definitions to templated categorical variable definitions
purrr::walk(artis_gen_tbl_names, \(a_gen_tbl_name) {
  
  # match to the specific catvars file with the general tbl name
  a_catvars_file <- list.files(
    path = path_templates,
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
  
  # # skip if no definitions exist for this table
  # if (nrow(artis_filter_catvars) == 0) {
  #   message(glue("No catvars definitions found for `{a_gen_tbl_name}` - skipping"))
  #   return(invisible(NULL))
  # }
  a_catvar_tbl <- read.delim(file.path(path_templates, a_catvars_file))
  
  a_catvar_tbl <- a_catvar_tbl %>% 
    select(-definition) %>% 
    left_join(
      artis_filter_catvars,
      by = c("attributeName", "code"))
  
  # add artis hs_version definitions if present in the attributeName column
  if(any(a_catvar_tbl$attributeName %in% c("hs_version"))){
    
    a_catvar_tbl <- a_catvar_tbl %>% 
      bind_rows(artis_defs_hs_v) %>%
      # coerce logical produced by is.na() to integer - FALSE = 0 TRUE = 1
      # arrange sorts ascending by default - puts 0 first - values with definitions
      arrange(attributeName, code, is.na(definition), definition == "") %>%
      # duplicate hs_version x code combos - keep ones with definition (show up first)
      distinct(attributeName, code, .keep_all = TRUE) %>%
      mutate(
        definition = case_when(
          attributeName == "hs_version" & code == "HS92" ~ "Harmonized System version 1992",
          .default = definition
        )
      )
    
    # NOTE: This intentionally skips the hs_version == "HS92" value in the reference_hs6 table in the join.
    # This value is filtered out in the ARTIS model run, but AM (2026-03-20) is intentionally choosing 
    # to manually add this definition in the reference_hs6 table since it will not be maintained in the artis_hs_version_dictionary.txt
  }
  
  # write out, replacing all automatically generated values
  write.table(
    a_catvar_tbl,
    file = file.path(path_templates, a_catvars_file),
    sep = "\t",
    row.names = FALSE,
    quote = FALSE,
    na = ""
  )
  message(glue("Updated `{a_catvars_file}` with ARTIS catvars definitions"))
})

# Geographic coverage  ---------------------------------------------------
# Not relevant for ARTIS global coverage

# Create geographic coverage (required when more than one geographic location
# is to be reported in the metadata).

# EMLassemblyline::template_geographic_coverage(
#   path = path_templates, 
#   data.path = path_data, 
#   data.table = "", 
#   lat.col = "",
#   lon.col = "",
#   site.col = "")

# Taxonomic coverage -----------------------------------------------------
# 2026-03-25 AM - not going to deal with this at the moment. Might be helpful for other 
# ARTIS tasks. Too overwhemling with thousands of taxa

# Create taxonomic coverage template (Not-required. Use this to report 
# taxonomic entities in the metadata)

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


# ARTIS datatable definitions  -------------------------------------------

artis_defs_tbl <- artis_defs_tbl %>% 
  arrange(datatable_general_name) %>% 
  pull(definition)

# Make EML from metadata templates --------------------------------------------

# Once all your metadata templates are complete call this function to create 
# the EML.

# FIXIT: These values could pull from ARTIS config file

EMLassemblyline::make_eml(
  path = path_templates,
  data.path = path_data,
  eml.path = path_eml, 
  dataset.title = "Aquatic Resource Trade in Species (ARTIS) v1.2 FAO", 
  temporal.coverage = c("1996", "2020"), 
  geographic.description = "Global coverage", 
  geographic.coordinates = c("90", "180", "-90", "-180"), 
  maintenance.description = "This dataset is intended to be updated annually when new FAO/BACI trade year data is made available.", 
  data.table = list.files(path_data), 
  data.table.name = tools::file_path_sans_ext(list.files(path_data)),
  data.table.description = artis_defs_tbl,
  #other.entity = c(""),
  #other.entity.name = c(""),
  #other.entity.description = c(""),
  #user.id = "",
  #user.domain = "", 
  package.id = ""
  )


# Post EAL EML corrections -----------------------------------------------

artis_parquet_files <- list.files(
  artis_files_path, 
  recursive = TRUE
)

# Post EAL EML corrections -----------------------------------------------
# The EAL workflow generates EML describing 8 representative .csv files.
# This section replaces those <dataTable> elements with ones describing
# the actual 148 parquet files that make up the ARTIS dataset on KNB.
# Strategy: clone each of the 8 EAL-generated <dataTable> elements,
# keeping the full <attributeList>, <entityDescription>, and all other
# metadata intact. Only the <physical>, <entityName>, and <entityDescription>
# child elements are replaced to reflect each parquet file.
# Consumption and trade <dataTable> templates are each cloned many times
# (once per parquet file), while reference table templates are cloned once each.

library(EML)

# Read the EAL-generated EML back in as an R list object
eml <- EML::read_eml(file.path(path_eml, ".xml"))

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
# This determines which EAL-generated <attributeList> template to reuse.
# Note: reference_hs6_taxa_resolution must be checked before reference_hs6
# to avoid the shorter pattern matching both.
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

# check that all files are matched to table types
#table(detect_table_type(artis_parquet_files), useNA = "always")

# Helper: build an <entityDescription> string for a parquet file.
# For consumption and trade files, append the HS version and year
# extracted from the filename (e.g. "HS02", "2002").
# Reference table files have no HS version or year in their filenames,
# so they receive the base template description unchanged.
make_entity_description <- function(template_dt, parquet_file) {
  base_desc  <- template_dt$entityDescription
  hs_version <- stringr::str_extract(parquet_file, "HS\\d{2}")
  year       <- stringr::str_extract(parquet_file, "\\d{4}(?=\\.parquet)")

  if (is.na(hs_version) && is.na(year)) {
    base_desc
  } else {
    glue::glue("{base_desc} (This file contains a portion of the dataset HS version: {hs_version}, Year: {year})")
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

# Extract the 8 EAL-generated template <dataTable> elements from the EML,
# keyed by ARTIS table type name for lookup below.
# Note: reference_hs6 uses the .csv anchor to avoid matching reference_hs6_taxa_resolution.
orig_datatables <- eml$dataset$dataTable

get_template <- function(name_pattern) {
  idx <- which(sapply(orig_datatables, \(dt) grepl(name_pattern, dt$entityName)))
  orig_datatables[[idx]]
}

templates <- list(
  consumption                   = get_template("consumption$"),
  trade                         = get_template("trade$"),
  reference_countries           = get_template("reference_countries$"),
  reference_hs6_taxa_resolution = get_template("reference_hs6_taxa_resolution$"),
  reference_hs6                 = get_template("reference_hs6$"),
  reference_production          = get_template("reference_production$"),
  reference_sciname             = get_template("reference_sciname$"),
  reference_trade_baci          = get_template("reference_trade_baci$")
)

# Generate a <dataTable> element for each of the 148 parquet files.
# Each file is matched to its table type, then cloned from the corresponding
# template with an updated <physical>, <entityName>, and <entityDescription>.
# Files that fail type detection are dropped with a warning rather than 
# inserting NULL into the EML list.
parquet_datatables <- purrr::map(artis_parquet_files, \(f) {
  table_type <- detect_table_type(f)

  if (is.na(table_type)) {
    warning(glue::glue("Could not detect table type for: {f}. File was dropped from EML"))
    return(NULL)
  }

  make_parquet_datatable(templates[[table_type]], f, artis_files_path)
}) |>
  purrr::discard(is.null)

# Replace the 8 representative CSV <dataTable> elements with the 148
# parquet <dataTable> elements, then write out a new EML file.
# The original EAL-generated EML is preserved unchanged at "ARTIS_v1.2_FAO.xml".
eml$dataset$dataTable <- parquet_datatables

eml$dataset$additionalInfo <- "The consumption and trade tables are partitioned into 
individual parquet files by HS version and year. All consumption files share an identical 
schema, as do all trade files. The complete consumption and trade datasets are obtained by 
combining all files of each type."

EML::write_eml(eml, file.path(path_eml, "ARTIS_v1.2_FAO_parquet.xml"))

# validate 
EML::eml_validate(file.path(path_eml, "ARTIS_v1.2_FAO_parquet.xml"))
