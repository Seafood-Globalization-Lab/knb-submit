# Inspect a KNB data package 

# This script is not part of the EML and KNB publishing pipeline
# It is intended to save useful code to look at a data package hosted on KNB
# This can be useful in understanding how EML and data package resource maps are 
# formated and organized on the KNB data repository. 

# This script is intended to document the corrections made to the ARTIS v1.2 FAO 
# dataset on the KNB staging node before publishing on 2026-04-24 by AM. 

# Used NCEAS data team training doc as guide - https://nceas.github.io/datateam-training/reference/update-packages-with-datapack.html

# Set KNB stagging node credentials --------------------------------------

# 1) navigate to https://dev.nceas.ucsb.edu/ (may need to use a different browser)
# 2) login with your ORCid
# 3) navigate to your profile, settings, authentication Token, and "Token for DataONE R" 
# 4) copy the string to your clipboad. Paste and call in your console to set temporary access token

# (if having problems, click small red link on login popup window and adjust browser settings)
# Check out dataone r package vignette for more detailed instructions 
# https://cran.r-project.org/web/packages/dataone/vignettes/v02-dataone-federation.html
# Note the vignette instructions are for the productions site, we need a stagging site token

# Required packages --------------------------------

{
  pak::repo_add(CRAN = "RSPM@2026-01-01")
  pak::pak(c(
    "dataone",
    "datapack",
    "uuid",
    "dataone",
    "EML",
    "nceas/arcticdatautils",
    "glue"
  ))
  library(dataone)
  library(datapack)
  library(uuid)
  library(dataone)
  library(arcticdatautils)
  library(EML)
  library(glue)
}

# read in from ./config.yml
cfg <- config::get()

# Set ARTIS paths ----------------------------------------------------

# local file path to dataset root directory 
path_artis_files  <- Sys.getenv("ARTIS_DB_PATH")
path_metadata_dir <- "./artis_metadata_files"
# path to EML file generated in this project repo
path_artis_eml <- file.path(path_metadata_dir, "eml", glue("ARTIS_{cfg$model_version}_{cfg$prod_type}_EML.xml"))
# to inspect
eml <- EML::read_eml(path_artis_eml)

# Correct staged ARIS v1.2 KNB dataset -----------------------------------
# 2026-04-24

## Get KNB EML metadata -----------------------------------
# stagging ARTIS 
d1c_test <- D1Client("STAGING", "urn:node:mnTestKNB")
# staged ARTIS v1.2 (most recent version of the resource map ID)
resourcemapId <- "resource_map_urn:uuid:b0d0c7fc-c8e0-448b-ae44-856a6b1f4d3c"

# Download only the system metadata and EML metadata - NO data
dp <- getDataPackage(
  d1c_test,
  identifier = resourcemapId,
  lazyLoad = TRUE, # will not download data
  quiet = FALSE
)
metadataId <- selectMember(dp, name="sysmeta@formatId", value="https://eml.ecoinformatics.org/eml-2.2.0") # Get metadata PID
doc <- read_eml(getObject(d1c_test@mn, metadataId))

## Replace metadata -----------------------------------

# updated 01 and 02 scripts and data dictionaries, regenerated EML. Validate
eml_validate(eml)

# replace EML in data package
dp <- replaceMember(dp, metadataId, replacement = path_artis_eml)

# Push updates to KNB record
packageId <- dataone::uploadDataPackage(
  d1c_test, dp, public=TRUE, quiet=FALSE)
