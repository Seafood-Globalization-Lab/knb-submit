# Inspect a KNB data package 

# This script is not part of the EML and KNB publishing pipeline
# It is intended to save useful code to look at a data package hosted on KNB
# This can be useful in understanding how EML and data package resource maps are 
# formated and organized on the KNB data repository. 

# This script is intended to document the corrections made to the ARTIS v1.2 FAO 
# dataset on the KNB staging node before publishing on 2026-04-24 by AM. 

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
    "nceas/arcticdatautils"
  ))
  library(dataone)
  library(datapack)
  library(uuid)
  library(dataone)
  library(arcticdatautils)
  library(EML)
}

# set location
d1c <- D1Client("STAGING", "urn:node:mnTestKNB")
# resource map uuid
packageId <- "resource_map_urn:uuid:2fc365a1-38d2-442e-b848-6a4e9fbab6fa"
# Download only the system metadata and EML metadata - NO data
dp <- getDataPackage(
  d1c,
  identifier = packageId,
  lazyLoad = TRUE, # will not download data
  quiet = FALSE
)
    
#get metadata id
# This is erroring out - expecting XML but returning JSON. IDK why, just copied value in return text for EML object
#metadataId <- selectMember(dp, name="sysmeta@formatId", value="https://eml.ecoinformatics.org/eml-2.2.0")
metadataId <- "urn:uuid:303ff3c3-0f0e-444c-89b6-1d3f2be7bd3c"

# read the EML as R object to inspect
doc <- read_eml(getObject(d1c@mn, metadataId))

########### Test example 
# does addMember() add identifier value? - No I don't think so. I think UUIDs are added by KNB once package is pushed up. 
# 

dpkg <- new("DataPackage")
data <- charToRaw("1,2,3\n4,5,6")
metadata <- charToRaw("EML or other metadata document text goes here\n")
md <- new("DataObject", id="md1", dataobj=metadata, format="text/xml", user="smith", 
  mnNodeId="urn:node:KNB")
do <- new("DataObject", id="id1", dataobj=data, format="text/csv", user="smith", 
  mnNodeId="urn:node:KNB")
# Associate the metadata object with the science object. The 'mo' object will be added 
# to the package  automatically, since it hasn't been added yet.
dpkg <- addMember(dpkg, do, md)


# Correct staged ARIS v1.2 KNB dataset -----------------------------------
# 2026-04-24

## Get KNB EML metadata -----------------------------------
# set location
d1c <- D1Client("STAGING", "urn:node:mnTestKNB")
# resource map uuid - Does not work - Error: ! XML content does not seem to be XML: '"q":"id:\"resource_map_urn:uuid:eff0aed4-7c15-4b8b-b53c-b94484e4e03c\""
resourcemapId <- "resource_map_urn:uuid:eff0aed4-7c15-4b8b-b53c-b94484e4e03c"

# Download only the system metadata and EML metadata - NO data
dp <- getDataPackage(
  d1c,
  identifier = resourcemapId,
  lazyLoad = TRUE, # will not download data
  quiet = FALSE
)
# the data package uuid also does not work, but gives us the EML identifier in the error message as "isDocumentedBy" value
packageId <- "urn:uuid:eff0aed4-7c15-4b8b-b53c-b94484e4e03c"
# Download only the system metadata and EML metadata - NO data
dp <- getDataPackage(
  d1c,
  identifier = packageId,
  lazyLoad = TRUE, # will not download data
  quiet = FALSE
)
# Set uuid for metadata from error message
metadataId <- "urn:uuid:eff0aed4-7c15-4b8b-b53c-b94484e4e03c"
eml <- read_eml(getObject(d1c@mn, metadataId))

## Make edits to local EML R list object -----------------------------------

# replace "my-dataset-tmp-id" text string identifier value
#eml$dataset$id <- dataone::generateIdentifier(d1c@mn, scheme = "uuid")

# write_eml(doc, eml_path)

# dp <- replaceMember(dp, metadataId, replacement=eml_path)

