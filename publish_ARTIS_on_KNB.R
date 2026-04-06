# Push draft ARTIS datapackage to KNB stagging node

# Required packages (with version control) --------------------------------

# Use pak to set software repo to Posit package manager and pin a
# snapshot date for reproducibility
pak::repo_add(CRAN = "RSPM@2025-10-01")
pak::pak(c(
  "dataone",
  "datapack",
  "uuid",
  "purrr"
))
library(dataone)
library(datapack)
library(uuid)
library(purrr)


# Set ARTIS data path ----------------------------------------------------

artis_files_path  <- Sys.getenv("ARTIS_DB_PATH")

# Set DataOne paths and config ------------------------------------------------------

# Set DataONE Coordinating Node
cn <- CNode("STAGING")
# Get reference to node based on its identifier
mn <- getMNode(cn,'urn:node:mnTestKNB')
# DataONE client class used to download, update, and search for data in the DataONE network
d1c_test <- D1Client(cn,mn)

# Build data package -----------------------------------------

# create a new data package
dp <- new("DataPackage")


# Add EML metadata -------------------------------------------------------

# path to EML 
emlFile <- file.path("metadata-files", "eml", "ARTIS_v1.2_FAO.xml")
# create a new DataObject for the metadata
metadataObj <- new(
  "DataObject", 
  format="https://eml.ecoinformatics.org/eml-2.2.0", 
  filename=emlFile)
# add the new DataObject to the package 
dp <- addMember(dp, metadataObj)


# Add data files ---------------------------------------------------------

artis_data_files <- list.files(artis_files_path, recursive = TRUE, full.names = TRUE)

# iterate through each ARTIS data file to add as an individual object to the data package
dp <- purrr::reduce(artis_data_files, \(dp, a_data_file) {

  sourceObj <- new("DataObject", format = "parquet", filename = a_data_file)
  addMember(dp, sourceObj, metadataObj)
}, .init = dp)



# Upload Data Package ----------------------------------------------------

# give privileges to KNb admins
myAccessRules <- data.frame(
  subject="CN=knb-admins,DC=dataone,DC=org", 
  permission="changePermission") 

packageId <- uploadDataPackage(
  d1c_test, dp, public=TRUE, accessRules=myAccessRules, quiet=FALSE)