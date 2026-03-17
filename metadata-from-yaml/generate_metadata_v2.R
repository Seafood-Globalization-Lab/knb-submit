# Generate Ecological Metadata Language (EML)

# Uses metadata_eml_template.yml YAML schema as the basis for deriving metadata values into
# a valid EML schema. Copy metadata_eml_templat.yml to create a custom file to input 
# metadata values relevant to users specific dataset being documented. 

# Uses packages emld and EML R packages to build EML

# Author: Althea Marks
# Date: 2026-02-13

# Basis of this script inspired by this function: 
# https://github.com/mlap/neon4cast-aquatics/blob/master/metadata/generate_metadata.R
# and YAML template: 
# https://github.com/mlap/neon4cast-aquatics/blob/master/metadata/metadata.yml


# setup ------------------------------------------------------------------
# Use pak to align package versions across users and time ++ reproducibility
# Install pak if you don't have it
# install.packages("pak")

# @ snapshot date - hold all package versions to the "latest" version at this date
pak::repo_add(CRAN = "RSPM@2025-10-01")
# check with
#pak::repo_get()

# Install packages from the snapshot - will not install if version is already present
pak::pak(c(
  "EML",
  "yaml",
  "emld",
  "xml2",
  "nceas/arcticdatautils",
  "dataone"
))
# load packages from library
library(EML)
library(yaml)
library(emld)
library(arcticdatautils)
library(dataone)

# read YAML metadata file ------------------------------------------------
eml_yml <- yaml::read_yaml("./metadata/metadata_artis.yml")

# Note: emld pkg does not contain a template of EML. 


# Example EML ------------------------------------------------------------

d1c_test <- dataone::D1Client("STAGING", "urn:node:mnTestARCTIC")
doc <- read_eml(getObject(d1c_test@mn, "urn:uuid:558eabf1-1e91-4881-8ba3-ef8684d8f6a1"))

# Define Funciton to remove empty fields ---------------------------------------------

# Remove NULL, empty strings, and empty lists recursively
# dives into list and cleans from the bottom up
clean_empty <- function(x) {
  # only run on lists, individual values skip to the 2nd if statement
  if (is.list(x)) {
    # calls function on every element of list - this is the recursive component
    x <- lapply(x, clean_empty)
    # remove null values
    x <- x[!sapply(x, is.null)]
    # remove empty string values
    x <- x[!sapply(x, function(i) is.character(i) && length(i) == 1 && i == "")]
    # remove empty lists
    x <- x[!sapply(x, function(i) is.list(i) && length(i) == 0)]
  }
  # If item NOT a list (i.e. a scalar value) - then run this conditional 
  if (length(x) == 0) return(NULL)
  x
} 


# Clean empty values from YAML values ------------------------------------

# clean the eml list object - clean every element recursively
eml_yml_clean <- clean_empty(eml_yml)

# Assign yml values to EML List -----------------------------------------------

# NOTE - AM 2026-03-17 

# Build EML document structure following EML 2.2.0 schema extracted with Claude from EML schema repo
eml_list <- list(
  
  # Dataset element (must follow schema order)
  dataset = list(
    # Basic identification
    title = eml_yml_clean$dataset$title,
    shortName = eml_yml_clean$dataset$shortName,
    
    # AlternateIdentifier (optional, can be multiple)
    alternateIdentifier = eml_yml_clean$dataset$alternateIdentifier,
    
    # Responsible parties - creator (required, can be multiple)
    creator = eml_yml_clean$dataset$creator,
    
    # Metadata provider (optional)
    metadataProvider = eml_yml_clean$dataset$metadataProvider,
    
    # Associated parties (optional)
    associatedParty = eml_yml_clean$dataset$associatedParty,
    
    # Publication info
    pubDate = eml_yml_clean$dataset$pubDate,
    language = eml_yml_clean$dataset$language,
    
    # Series (optional)
    series = eml_yml_clean$dataset$series,
    
    # Abstract (required)
    abstract = eml_yml_clean$dataset$abstract,
    
    # Keywords (optional, can be multiple sets)
    keywordSet = eml_yml_clean$dataset$keywordSet,
    
    # Additional info (optional)
    additionalInfo = eml_yml_clean$dataset$additionalInfo,
    
    # Intellectual rights (recommended)
    intellectualRights = eml_yml_clean$dataset$intellectualRights,
    
    # Licensed (optional, replaces intellectualRights in newer versions)
    licensed = eml_yml_clean$dataset$licensed,
    
    # Distribution/access (optional)
    distribution = eml_yml_clean$dataset$distribution,
    
    # Coverage (recommended)
    coverage = eml_yml_clean$dataset$coverage,
    
    # Maintenance (optional)
    maintenance = eml_yml_clean$dataset$maintenance,
    
    # Contact (required, can be multiple)
    contact = eml_yml_clean$dataset$contact,
    
    # Publisher (optional)
    publisher = eml_yml_clean$dataset$publisher,
    
    # Publishing details (optional)
    pubPlace = eml_yml_clean$dataset$pubPlace,
    
    # Methods (recommended)
    methods = eml_yml_clean$dataset$methods,
    
    # Project (optional)
    project = eml_yml_clean$dataset$project,
    
    # Data entities (at least one required for data packages)
    dataTable = eml_yml_clean$dataset$dataTable,
    spatialRaster = eml_yml_clean$dataset$spatialRaster,
    spatialVector = eml_yml_clean$dataset$spatialVector,
    storedProcedure = eml_yml_clean$dataset$storedProcedure,
    view = eml_yml_clean$dataset$view,
    otherEntity = eml_yml_clean$dataset$otherEntity
  ),
  # Root EML attributes — emld maps these to XML attributes on <eml>
  packageId = eml_yml_clean$packageId,
  system    = eml_yml_clean$system,
  scope     = eml_yml_clean$scope
)

# # quick test
# eml_list_test <- clean_empty(list(
#   dataset   = eml_yml$dataset,   # take the whole dataset subtree as-is
#   packageId = "doi:10.xxxx/PLACEHOLDER",
#   system    = "doi"
# ))

# # 
# #as_xml(eml_list_test, "./test_eml.xml")
# write_eml(eml_list_test, "./test_eml_eml.xml")
# eml_validate("./test_eml_eml.xml")

# Clean the structure again to remove any NULLs from optional fields
eml_list <- clean_empty(eml_list)
write_eml(eml_list, "./eml_write_from_yml.xml")
eml_validate("./eml_write_from_yml.xml")

# test reading file back in
test_read_eml <- read_eml("./eml_write_from_yml.xml")
class(test_read_eml) 
# [1] "emld" "list"

# AM 2026-03-17 - Validating "./eml_write_from_yml.xml" is throwoing errors - maybe I have the yaml subelements out of order?
# [1] FALSE
# attr(,"errors")
# [1] "Element 'address': This element is not expected. Expected is one of ( references, individualName, organizationName, positionName )."
# [2] "Element 'address': This element is not expected. Expected is one of ( references, individualName, organizationName, positionName )."
# [3] "Element 'address': This element is not expected. Expected is one of ( references, individualName, organizationName, positionName )."
# [4] "Element 'physical': This element is not expected. Expected is one of ( alternateIdentifier, entityName, references )."   



# Attempt 2 Build EML list from Yml values -------------------------------

# Use EMl / articutils to protect sublist structure?
# I don't think providing a list through `eml$element()` function works if I supply it with a list

eml_list <- list(
  
  #   # Root EML attributes — emld maps these to XML attributes on <eml>
  # packageId = eml$packageID(eml_yml$packageId),
  # system    = eml_yml$system,
  # scope     = eml_yml$scope,

  # Dataset element (must follow schema order)
  dataset = list(
    # Basic identification
    title = eml$title(value = eml_yml$dataset$title),
    #shortName = eml_yml$dataset$shortName,
    
    # AlternateIdentifier (optional, can be multiple)
    #alternateIdentifier = eml$alternateIdentifier(eml_yml$dataset$alternateIdentifier),
    
    # Responsible parties - creator (required, can be multiple)
    creator = eml$creator(individualName =  list(eml_yml$dataset$creator[[1]])),
    
    # Metadata provider (optional)
    metadataProvider = eml_yml$dataset$metadataProvider,
    
    # Associated parties (optional)
    associatedParty = eml_yml$dataset$associatedParty,
    
    # Publication info
    pubDate = eml_yml$dataset$pubDate,
    language = eml_yml$dataset$language,
    
    # Series (optional)
    series = eml_yml$dataset$series,
    
    # Abstract (required)
    abstract = eml_yml$dataset$abstract,
    
    # Keywords (optional, can be multiple sets)
    keywordSet = eml_yml$dataset$keywordSet,
    
    # Additional info (optional)
    additionalInfo = eml_yml$dataset$additionalInfo,
    
    # Intellectual rights (recommended)
    intellectualRights = eml_yml$dataset$intellectualRights,
    
    # Licensed (optional, replaces intellectualRights in newer versions)
    licensed = eml_yml$dataset$licensed,
    
    # Distribution/access (optional)
    distribution = eml_yml$dataset$distribution,
    
    # Coverage (recommended)
    coverage = eml_yml$dataset$coverage,
    
    # Maintenance (optional)
    maintenance = eml_yml$dataset$maintenance,
    
    # Contact (required, can be multiple)
    contact = eml_yml$dataset$contact,
    
    # Publisher (optional)
    publisher = eml_yml$dataset$publisher,
    
    # Publishing details (optional)
    pubPlace = eml_yml$dataset$pubPlace,
    
    # Methods (recommended)
    methods = eml_yml$dataset$methods,
    
    # Project (optional)
    project = eml_yml$dataset$project,
    
    # Data entities (at least one required for data packages)
    dataTable = eml_yml$dataset$dataTable,
    spatialRaster = eml_yml$dataset$spatialRaster,
    spatialVector = eml_yml$dataset$spatialVector,
    storedProcedure = eml_yml$dataset$storedProcedure,
    view = eml_yml$dataset$view,
    otherEntity = eml_yml$dataset$otherEntity
  )
)

