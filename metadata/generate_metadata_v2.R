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
  "xml2"
))
# load packages from library
library(EML)
library(yaml)
library(emld)

# read YAML metadata file ------------------------------------------------
eml_yml <- yaml::read_yaml("./metadata/metadata_artis.yml")

# Note: emld pkg does not contain a template of EML. 


# Fun to remove empty fields ---------------------------------------------

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

# clean the eml list object - clean every element recursively
eml_yml_clean <- clean_empty(eml_yml)

# assign yml values to EML -----------------------------------------------

# Build EML document structure following EML 2.2.0 schema
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

# Validating "./eml_write_from_yml.xml" 





