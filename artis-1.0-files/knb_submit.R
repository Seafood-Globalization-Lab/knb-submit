
library(dataone)
library(arcticdatautils)

# Credentials
# Get authentication token from KNB sign in -> My Profile -> Settings -> Authentication Token
# Once you have your KNB token add line KNB_TOKEN="YOUR_KNB_TOKEN_HERE" to your .Renviron file and restart R session
options(dataone_token = Sys.getenv("KNB_TOKEN"))


# Instructions for making a public data package private-------------------------
# Assumption: a KNB data package has already been created via GUI online
# Note:
  # When a KNB data package is initially created, via GUI, it will be public
  # Follow these instructions to make it private for staging

# Connecting to KNB internal database
knb <- D1Client("PROD", "urn:node:KNB")
# Reference to specific data package within KNB
# Note:
  # This ID number comes from the top of the KNB data package created via GUI
  # this is in the panel above files and folders in a section starting "Files in this dataset"
  # the id number should start with "resource_map_urn"
resource_map_pid <- "resource_map_urn:uuid:2ba3c6ba-3d2a-49fe-b98d-ecbf5a4646b3"
# Getting KNB data package via database client and data package ID number
# FIXIT: currently using deprecated function, current version is pkg <- dataone::getDataPackage(knb, resource_map_pid) 
pkg <- get_package(knb@mn, resource_map_pid)
# Converting data package from public (everyone can view) to private (only authors can view and edit)
remove_public_read(knb@mn, c(pkg$metadata, pkg$data, pkg$resource_map))

# Instructions for uploading data to KNB data package---------------------------
# Assumption: A KNB data package has already been created via GUI

artis_outputs_dir <- "qa/repo_data/zenodo_archive_database_20240422"
snet_files <- list.files(path = artis_outputs_dir, pattern = "snet", full.names = TRUE)
consumption_files <- list.files(path = artis_outputs_dir, pattern = "consumption", full.names = TRUE)

# Getting dataone package instance for upload
pkg <- dataone::getDataPackage(knb, resource_map_pid)


# get current metadata identifier
metadataId <- selectMember(dp, name="sysmeta@formatId", value="https://eml.ecoinformatics.org/eml-2.2.0")

# replace metadata file with a local edited version
# dp <- replaceMember(dp, metadataId, replacement="qa/metadata/Aquatic_Resource_Trade_in_Species.xml")

# data file (CSV) being uploaded
sourceObj <- new("DataObject", format="text/csv", filename="qa/repo_data/zenodo_archive_database_20240422/snet_HS02_y2004.csv")

# Adding new data file to data package
pkg <- addMember(pkg, sourceObj, metaObj) # The third argument of addMember() associates the new DataObject to the metadata

# Upload changes (new files) to package
uploadDataPackage(knb, pkg, public = FALSE, accessRules=pkg@sysmeta@accessPolicy, quiet = FALSE)



