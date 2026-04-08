# Push draft ARTIS datapackage to KNB stagging node

# Required packages (with version control) --------------------------------

#+ Use pak to set software repo to Posit package manager and pin a
# snapshot date for reproducibility
pak::repo_add(CRAN = "RSPM@2025-10-01")
pak::pak(c(
  "dataone",
  "datapack",
  "uuid",
  "purrr",
  "EML"
))
library(dataone)
library(datapack)
library(uuid)
library(purrr)
library(EML)


# Set ARTIS paths ----------------------------------------------------

# local file path to dataset root directory 
path_artis_files  <- Sys.getenv("ARTIS_DB_PATH")
# path to EML file generated in this project repo
path_artis_eml <- file.path("metadata-files", "eml", "ARTIS_v1.2_FAO_EML.xml")


# Load my helper functions --------------------------------------------------

source("./functions/ARTIS_EAL_helper_functions.R")

# Point to DataOne ------------------------------------------------------

# Set DataONE Coordinating Node - We use staging to work with KNB curators before publishing
cn <- CNode("STAGING")
# Get reference to node based on its identifier
mn <- getMNode(cn,'urn:node:mnTestKNB')
# DataONE client class used to download, update, and search for data in the DataONE network
d1c_test <- D1Client(cn,mn)

# Build data package -----------------------------------------

# create a new data package
dp <- new("DataPackage")

# create a new DataObject for the metadata
metadataObj <- new(
  "DataObject", 
  format="https://eml.ecoinformatics.org/eml-2.2.0", 
  filename=path_artis_eml)
# add the new DataObject to the package 
dp <- addMember(dp, metadataObj)

# Add ARTIS data files to data package ---------------------------------------------------------

artis_data_files <- list.files(path_artis_files, recursive = TRUE, full.names = TRUE)

# Iterate through each ARTIS data file and add as an individual DataObject
# purrr::reduce() threads the updated dp forward through each iteration
dp <- purrr::reduce(artis_data_files, \(dp, a_data_file) {
  sourceObj <- new(
    "DataObject",
    format   = get_format(a_data_file),
    filename = a_data_file
  )
  addMember(dp, sourceObj, metadataObj)
}, .init = dp)

# Test data package data objects -----------------------------------------

# Dynamically detect file extensions from dp@objects and artis_data_files
# and compare counts to ensure all files were added correctly
dp_formats <- table(sapply(dp@objects, \(obj) tools::file_ext(obj@sysmeta@fileName)))
artis_formats <- table(tools::file_ext(c(basename(artis_data_files), basename(path_artis_eml))))

# Build readable summary strings from detected extensions
format_summary <- function(format_table) {
  paste(
    sapply(names(format_table), \(ext) {
      glue::glue("{format_table[[ext]]} .{ext} file{ifelse(format_table[[ext]] > 1, 's', '')}")
    }),
    collapse = ", "
  )
}

if (all(dp_formats == artis_formats)) {
  message(glue::glue(
    "Data package check passed: {length(dp@objects)} total members\n",
    "  artis_data_files: {format_summary(artis_formats)}\n",
    "  dp@objects:       {format_summary(dp_formats)}"
  ))
} else {
  stop(glue::glue(
    "Data package member count mismatch:\n",
    "  artis_data_files: {format_summary(artis_formats)}\n",
    "  dp@objects:       {format_summary(dp_formats)}"
  ))
}

# Verify data package member count matches EML dataTable + otherEntity count
# This ensures the EML document and the data package are in sync before upload
eml_for_check  <- EML::read_eml(path_artis_eml)
eml_data_count <- length(eml_for_check$dataset$dataTable) + 
                  length(eml_for_check$dataset$otherEntity$entityName)

# dp@objects includes the EML metadata object itself which is not listed
# as a dataTable or otherEntity in the EML — subtract 1 for the comparison
dp_data_count <- length(dp@objects) - 1L

if (dp_data_count == eml_data_count) {
  message(glue::glue(
    "EML and data package are in sync: {eml_data_count} total entities\n",
    "  EML entities:     {length(eml_for_check$dataset$dataTable)} dataTables + ",
    "{length(eml_for_check$dataset$otherEntity$entityName)} otherEntities\n",
    "  dp@objects:       {dp_data_count} data objects (excluding EML metadata object)"
  ))
} else {
  stop(glue::glue(
    "EML and data package are out of sync:\n",
    "  EML entities:     {length(eml_for_check$dataset$dataTable)} dataTables + ",
    "{length(eml_for_check$dataset$otherEntity$entityName)} otherEntities = {eml_data_count} total\n",
    "  dp@objects:       {dp_data_count} data objects (excluding EML metadata object)\n",
    "  Check that all parquet files and other entities were added to both the EML and the data package."
  ))
}

# Upload Data Package ----------------------------------------------------

# give privileges to KNb admins
myAccessRules <- data.frame(
  subject="CN=knb-admins,DC=dataone,DC=org", 
  permission="changePermission") 

### Need to have authentication token from http://dev.nceas.ucsb.edu/
## Login with ORCiD 
## (if having problems, click small red link on login popup window and adjust browser settings)
## Check out dataone r package vignette for more detailed instructions 
## https://cran.r-project.org/web/packages/dataone/vignettes/v02-dataone-federation.html
## Note the vignette instructions are for the productions site, we need a stagging site token

packageId <- uploadDataPackage(
  d1c_test, dp, public=TRUE, accessRules=myAccessRules, quiet=FALSE)
