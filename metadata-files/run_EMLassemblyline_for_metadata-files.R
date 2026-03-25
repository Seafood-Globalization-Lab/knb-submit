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

# Initialize workspace --------------------------------------------------------

# Update EMLassemblyline and load
# use pak to set software repo to Posit package manager and pin a 
# snapshot date for reproducabilty 
pak::repo_add(CRAN = "RSPM@2025-10-01")
pak::pak(c(
  "EDIorg/EMLassemblyline",
  "usethis",
  "arrow"
))
library(EMLassemblyline)
library(usethis)
library(arrow)

# set path to pre-released ARTIS dataset locally on AM's machine in 
# .Renviron file at project level
#edit_r_environ(scope = c("project"))
artis_files_path <- Sys.getenv("ARTIS_DB_PATH")

# Define paths for your metadata templates, data, and EML
path_templates <- "./metadata-files"
path_data <- file.path(artis_files_path)
path_eml <- ""

# load helper functions
source("./functions/eml_helper_functions.R")
# Create metadata templates ---------------------------------------------------

# Below is a list of boiler plate function calls for creating metadata templates.
# They are meant to be a reminder and save you a little time. Remove the 
# functions and arguments you don't need AND ... don't forget to read the docs! 
# E.g. ?template_core_metadata
# can rerun - will not overwrite

# Create core templates (required for all data packages)

EMLassemblyline::template_core_metadata(
  path = path_templates,
  license = "CCBY",
  file.type = ".md")

# Create table attributes template (required when data tables are present)
# function is not compatible with parquet file format. Created helper functions 
# to replicate this functionality for parquet files.
# EMLassemblyline::template_table_attributes(
#   path = path_templates,
#   data.path = file.path(path_data, "reference_tables"),
#   data.table = c("ARTIS_v1.2_FAO_reference_baci_trade.csv"))

# testing to figure out custom_units.txt generation behavior for multiple or single files
EMLassemblyline::template_table_attributes(
  path = path_templates,
  data.path = file.path(path_data, "reference_tables")#,
  #data.table = c("ARTIS_v1.2_FAO_reference_hs6.csv")
)
# looks like it writes a single custom_units.txt for 6 reference tables


# gather vector of files to document
# all reference tables, a sinlge represenative trade and consumption file
my_data_tables <- list.files(
  file.path(path_data, "reference_tables"),
  full.names = TRUE
) %>% c(
  list.files(
    file.path(path_data, "trade", "HS96"),
    full.names = TRUE)[1]
) %>% c(
  list.files(
    file.path(path_data, "consumption", "HS96"),
  full.names = TRUE)[1]
)

# run help_write_eml_attributes() over each file to generate EML attributes template
purrr::walk(my_data_tables, \(file_path) {
  base_name <- tools::file_path_sans_ext(basename(file_path))
  
  # strip _HS##_YYYY suffix from trade and consumption filenames
  base_name <- sub("_HS\\d+_\\d+", "", base_name)
  
  out_path <- file.path(path_templates, paste0("attributes_", base_name, ".txt"))
  
  ds <- arrow::open_dataset(file_path)
  
  help_write_eml_attributes(
    data_table = ds,
    out_path   = out_path
  )
})

# edit values manually in spreadsheet

# view standard unit descriptions
view_unit_dictionary()

# Create categorical variables template (required when attributes templates
# contains variables with a "categorical" class)

EMLassemblyline::template_categorical_variables(
  path = path_templates, 
  data.path = file.path(path_data, "reference_tables")
)

# Create geographic coverage (required when more than one geographic location
# is to be reported in the metadata).

EMLassemblyline::template_geographic_coverage(
  path = path_templates, 
  data.path = path_data, 
  data.table = "", 
  lat.col = "",
  lon.col = "",
  site.col = "")

# Create taxonomic coverage template (Not-required. Use this to report 
# taxonomic entities in the metadata)

remotes::install_github("EDIorg/taxonomyCleanr")
library(taxonomyCleanr)

taxonomyCleanr::view_taxa_authorities()

EMLassemblyline::template_taxonomic_coverage(
  path = path_templates, 
  data.path = path_data,
  taxa.table = "",
  taxa.col = "",
  taxa.name.type = "",
  taxa.authority = 3)

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
