# Push draft ARTIS datapackage to KNB stagging node

# Restart R session ------------------------------------

# start with a clean working envirnoment 


# Set KNB stagging node credentials --------------------------------------

# 1) navigate to https://dev.nceas.ucsb.edu/ (may need to use a different browser)
# 2) login with your ORCid
# 3) navigate to your profile, settings, authentication Token, and "Token for DataONE R" 
# 4) copy the string to your clipboad. Paste and call in your console to set temporary access token

# Required packages (with version control) --------------------------------

# Use pak to set software repo to Posit package manager and pin a
# snapshot date for reproducibility
{
 pak::repo_add(CRAN = "RSPM@2025-10-01")
  pak::pak(c(
    "dataone",
    "datapack",
    "uuid",
    "purrr",
    "EML",
    "config",
    "glue",
    "arcticdatautils"
  ))
  library(dataone)
  library(datapack)
  library(uuid)
  library(purrr)
  library(EML) 
  library(config)
  library(glue)
  library(arcticdatautils)
}

# Get config values ------------------------------------------------------

# read in from ./config.yml
cfg <- config::get()

# Set ARTIS paths ----------------------------------------------------

# local file path to dataset root directory 
path_artis_files  <- Sys.getenv("ARTIS_DB_PATH")
path_metadata_dir <- "./artis_metadata_files"
# path to EML file generated in this project repo
path_artis_eml <- file.path(path_metadata_dir, "eml", glue("ARTIS_{cfg$model_version}_{cfg$prod_type}_EML.xml"))
# to inspect
#eml <- EML::read_eml(path_artis_eml)


# Load my helper functions --------------------------------------------------

source("./functions/ARTIS_EAL_helper_functions.R")

# Point to DataOne ------------------------------------------------------

# Set DataONE Coordinating Node - We use staging to work with KNB curators before publishing
cn <- CNode(cfg$dataone_coordinating_node)
# Get reference to node based on its identifier
mn <- getMNode(cn,cfg$dataone_member_node)
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

# Extract the metadata PID string from the already-constructed metadataObj
metadataId <- metadataObj@sysmeta@identifier

# Add ARTIS data files to data package ---------------------------------------------------------

# get the absolute file paths to local artis data files
path_abs_artis_data_files <- list.files(path_artis_files, recursive = TRUE, full.names = TRUE)
# get the relative file paths (folder architecture) 
path_rel_artis_data_files <- list.files(path_artis_files, recursive = TRUE, full.names = FALSE)

# Iterate through each ARTIS data file and add as an individual DataObject

# Step 1: build and inspect the list of DataObjects first
# This is slow because it makes requests to KNB for every file
data_objects <- purrr::map2(
  path_abs_artis_data_files,
  path_rel_artis_data_files,
  \(abs_path, rel_path) {
    formatId <- arcticdatautils::guess_format_id(abs_path)
    id       <- generateIdentifier(d1c_test@mn, scheme = "uuid")
    new(
      "DataObject",
      format     = formatId,
      filename   = abs_path,
      targetPath = rel_path,
      id         = id
    )
  }
)

# Inspect before committing — e.g. check targetPath for file architecture on KNB

# Step 2: once happy, add list of dataObjs to dp data package
for (dataObj in data_objects) {
  dp <- addMember(dp, dataObj, metadataId)
}

# Test data package data objects -----------------------------------------

# Dynamically detect file extensions from dp@objects and path_abs_artis_data_files
# and compare counts to ensure all files were added correctly
dp_formats <- table(sapply(dp@objects, \(obj) tools::file_ext(obj@sysmeta@fileName)))
artis_formats <- table(tools::file_ext(c(basename(path_abs_artis_data_files), basename(path_artis_eml))))

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
    "  path_abs_artis_data_files: {format_summary(artis_formats)}\n",
    "  dp@objects:       {format_summary(dp_formats)}"
  ))
} else {
  stop(glue::glue(
    "Data package member count mismatch:\n",
    "  path_abs_artis_data_files: {format_summary(artis_formats)}\n",
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
