# This script executes an EMLassemblyline workflow.

# created 2026-03-23 by Althea Marks

# This script is intendied to be used once to initialize and set up an 
# EML assembly line workflow to generate the EML metadata for the release of 
# ARTIS on KNB data repository. 

# Note - a second script will be used to update future releases of ARTIS.
# this script serves as documentation of the inital setup. 

# Create worflow template ------------------------------------------------

# create directory structure for data package contents and EMLassemblyline
# created the template of this script. 
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
  "glue"
))
library(EMLassemblyline)
library(usethis)
library(arrow)
library(dplyr)
library(tools)
library(stringr)
library(glue)


# File Paths -------------------------------------------------------------

# set path to pre-released ARTIS dataset locally on AM's machine in 
# .Renviron file at project level
#usethis::edit_r_environ(scope = c("project"))
artis_files_path <- Sys.getenv("ARTIS_DB_PATH")

# Define paths for your metadata templates, data, and EML
path_metadata_dir <- "./metadata-files"

path_templates <- file.path(path_metadata_dir, "metadata_templates")
# where ARTIS .csv data subset will be writen to
path_data <- file.path(path_metadata_dir, "data_objects")
path_eml <- file.path(path_metadata_dir, "eml")


# Read in ARTIS definitions ----------------------------------------------
# read in long-lived ARTIS data dictionaries (attribute definitions)
# Most values will not need updating between release versions unless new columns 
# are added or names are changed.
# Open file in a spreadsheet (excel) if edits are needed. 

artis_attr_defs <- read.delim("./artis_data_dictionary_attributes.txt")
artis_attr_catvars_defs <- read.delim("./artis_data_dictionary_attributes_catvars.txt")

# Load helper functions ----------------------------------------------
source("./functions/ARTIS_EAL_helper_functions.R")

# Setup .csv ARTIS files for EMLassemblyline ------------------------

# get the filepaths to select representative ARTIS data files
paths_artis_subset <- get_parquet_data_subset(
  path_data_dir = artis_files_path
)
# take artis parquet file paths and convert to csv writen in this metadata repo/directory
# This WILL write over existing files if the names are the same
convert_artis_to_csv(
  paths_artis_parquet = paths_artis_subset, 
  path_write_csv = path_data)

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

# Join ARTIS data defitions to temlplates -----------------------------------------------

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
  artis_filter_defs <- artis_attr_defs %>% 
    filter(datatable_general_name == a_gen_tbl_name) %>% 
    select(-c(datatable_general_name))

  an_attr_tbl <- an_attr_tbl %>% 
    left_join(
      artis_filter_defs,
      by = c("attributeName")) %>% 
    # ensure no extra NA values are introduced
    mutate(missingValueCodeExplanation = "")
  
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


# Categorical variables --------------------------------------------------
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
  artis_filter_catvars <- artis_attr_catvars_defs |>
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

# Make EML from metadata templates --------------------------------------------

# Once all your metadata templates are complete call this function to create 
# the EML.

EMLassemblyline::make_eml(
  path = path_templates,
  data.path = path_data,
  eml.path = path_eml, 
  dataset.title = "", 
  temporal.coverage = c("YYYY-MM-DD", "YYYY-MM-DD"), 
  geographic.description = "", 
  geographic.coordinates = c("N", "E", "S", "W"), 
  maintenance.description = "", 
  data.table = c(""), 
  data.table.name = c(""),
  data.table.description = c(""),
  other.entity = c(""),
  other.entity.name = c(""),
  other.entity.description = c(""),
  user.id = "",
  user.domain = "", 
  package.id = "")
