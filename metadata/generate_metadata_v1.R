# Generate EML metadata for ARTIS dataset

# Notes ------------------------------------------------------

## Take a look at Carl Boettiger's EML function in this project: 
# https://github.com/mlap/neon4cast-aquatics/blob/master/metadata/generate_metadata.R
# I like this one a lot. Have separate .yml file to hold metadata values. Better to edit
# collaboratively and version separate from code. Then R script reads in .yml and generates EML structure.
# Can include template in other analysis repos - analysis template. 


## EDI maintained ENLassemblyline package that has functions to help generate EML:
# https://ediorg.github.io/EMLassemblyline/articles/create_tmplts.html
# funcitons scan data and generate templates for attributes, coverage, etc.
# not a CRAN release so dependencies would be less ideal

library(eml)
library(arrow)

# Simplified EML structure:
# - eml
#   - dataset
#     - creator
#     - title
#     - publisher
#     - pubDate
#     - keywords
#     - abstract 
#     - intellectualRights
#     - contact
#     - methods
#     - coverage
#       - geographicCoverage
#       - temporalCoverage
#       - taxonomicCoverage
#     - dataTable
#       - entityName
#       - entityDescription
#       - physical
#       - attributeList

# take a look at the data structure
artis_fp <- file.path("~/Documents/UW-SAFS/ARTIS/data/outputs_1.2.0_SAU_2025-11-14/KNB")
# Open the Parquet dataset (does not read it into memory)
artis_trade_ds <- open_dataset(file.path(artis_fp, "ARTIS_v1.2.0_trade_SAU_mid_all_HS_yrs_2025-12-01.parquet"))
# Inspect schema and number of rows
glimpse(artis_trade_ds)

# Claude template for ARTIS SAU Trade EML metadata --------------------------------
library(EML)
library(dplyr)

# Define attribute metadata using tribble for readability
# Using the SQL table definitions and adding better ISO3C documentation
attributes <- tribble(
  ~attributeName         , ~attributeDefinition                                                                                                                             , ~formatString , ~unit   , ~numberType , ~domain            ,
  "year"                 , "Year in which trade took place"                                                                                                                 , "YYYY"        , NA      , "integer"   , "dateTimeDomain"   ,
  "hs_version"           , "Harmonized System (HS) classification version"                                                                                                  , NA            , NA      , NA          , "textDomain"       ,
  "source_country_iso3c" , "ISO 3166-1 alpha-3 three-letter country code for the country that produced the specific product (i.e., fishing nation or aquaculture producer)" , NA            , NA      , NA          , "externalCodeSet"  ,
  "exporter_iso3c"       , "ISO 3166-1 alpha-3 three-letter country code for direct exporter country"                                                                       , NA            , NA      , NA          , "externalCodeSet"  ,
  "importer_iso3c"       , "ISO 3166-1 alpha-3 three-letter country code for direct importer country"                                                                       , NA            , NA      , NA          , "externalCodeSet"  ,
  "hs6"                  , "HS 6-digit code used to identify what product is being traded"                                                                                  , NA            , NA      , NA          , "textDomain"       ,
  "sciname"              , "Species/species group name traded under the specific HS product and 6-digit code"                                                               , NA            , NA      , NA          , "textDomain"       ,
  "habitat"              , "Habitat in which species/species group was produced"                                                                                            , NA            , NA      , NA          , "enumeratedDomain" ,
  "method"               , "Defines method of production"                                                                                                                   , NA            , NA      , NA          , "enumeratedDomain" ,
  "dom_source"           , "Identifies the source for the export"                                                                                                           , NA            , NA      , NA          , "enumeratedDomain" ,
  "product_weight_t"     , "Product weight of trade record, in tonnes"                                                                                                      , NA            , "tonne" , "double"    , "numericDomain"    ,
  "live_weight_t"        , "Live weight equivalent of trade record, in tonnes"                                                                                              , NA            , "tonne" , "double"    , "numericDomain"
)

# Define factors for categorical variables
# FIXIT - How does ARTIS handle brackish habitat? Should include this in definition
habitat_factors <- data.frame(
  attributeName = "habitat",
  code = c("marine", "inland", "unknown"),
  definition = c(
    "Marine organism", 
    "Freshwater organism", 
    "Unknown habitat"
  )
)

method_factors <- data.frame(
  attributeName = "method",
  code = c("aquaculture", "capture", "unknown"),
  definition = c(
    "Produced via aquaculture", 
    "Produced via wild capture", 
    "Unknown production method"
  )
)

dom_source_factors <- data.frame(
  attributeName = "dom_source",
  code = c("domestic", "foreign", "error"),
  definition = c(
    "Domestic export produced and exported from the same country",
    "Foreign export imported from one country and then exported to another country or back to the original exporting country",
    "Error export that cannot be explained by domestic or foreign export records nor production records"
  )
)

# Combine all factor definitions
factors <- rbind(
  habitat_factors,
  method_factors,
  dom_source_factors
)

# Add external code set information for ISO3C codes
# This approach properly documents that these codes are from the ISO 3166-1 standard
for (iso_attr in c("source_country_iso3c", "exporter_iso3c", "importer_iso3c")) {
  attributes[attributes$attributeName == iso_attr, "codesetName"] <- "ISO 3166-1 alpha-3"
  attributes[attributes$attributeName == iso_attr, "codesetURL"] <- "https://www.iso.org/iso-3166-country-codes.html"
  attributes[attributes$attributeName == iso_attr, "definition"] <- "International standard three-letter country codes"
}

#FIXIT: Use measurementScale to indicate Interval when numeric? Can see examples in EML documentation

# Create attributeList using set_attributes
attributeList <- set_attributes(attributes, factors, 
                              col_classes = c(
                                "integer",   # year - INT
                                "character", # hs_version - VARCHAR(4)
                                "character", # source_country_iso3c - VARCHAR(7)
                                "character", # exporter_iso3c - VARCHAR(3)
                                "character", # importer_iso3c - VARCHAR(3)
                                "character", # hs6 - VARCHAR(6)
                                "character", # sciname - VARCHAR(100)
                                "factor",    # habitat - VARCHAR(100)
                                "factor",    # method - VARCHAR(100)
                                "factor",    # dom_source - VARCHAR(100)
                                "numeric",   # product_weight_t - FLOAT
                                "numeric"    # live_weight_t - FLOAT
                              ))

# Define physical file information
physical <- set_physical(
  objectName = "ARTIS_v1.2.0_trade_SAU_mid_all_HS_yrs_2025-12-01.parquet",
  dataFormat = list(
    externallyDefinedFormat = list(
      formatName = "Parquet"
    )
  ),
  size = list(
    size = "Provide file size here", 
    unit = "byte"
  )
)

# Create dataTable element
dataTable <- list(
  entityName = "ARTIS_v1.2.0_trade_SAU_mid_all_HS_yrs_2025-12-01.parquet",
  entityDescription = "Global seafood trade data with species attribution from Sea Around Us",
  physical = physical,
  attributeList = attributeList,
  numberOfRecords = 95307018
)

# Define coverage information
coverage <- set_coverage(
  begin = '2002-01-01', 
  end = '2025-12-01',
  sci_names = c("Various marine and freshwater species"),  # This would be expanded with actual species names
  geographicDescription = "Global coverage of seafood trade between countries",
  west = -180, 
  east = 180, 
  north = 90, 
  south = -90
)

# Create a person for the creator
creator <- list(
  individualName = list(
    givenName = "Your", 
    surName = "Name"
  ),
  electronicMailAddress = "your.email@example.com",
  organizationName = "Your Organization"
)

# Create additional researchers
associatedParty <- list(
  list(
    individualName = list(
      givenName = "Colleague", 
      surName = "Name"
    ),
    electronicMailAddress = "colleague@example.com",
    role = "Researcher",
    organizationName = "Collaborator Organization"
  ),
  list(
    individualName = list(
      givenName = "Another", 
      surName = "Researcher"
    ),
    electronicMailAddress = "another@example.com",
    role = "Collaborator",
    organizationName = "Another Organization"
  )
)

# Create address information
address <- list(
  deliveryPoint = "School of Aquatic and Fishery Sciences",
  city = "Seattle",
  administrativeArea = "WA",
  postalCode = "98195",
  country = "USA"
)

# Create publisher information
publisher <- list(
  organizationName = "Seafood Globalization Lab",
  address = address
)

# Define contact information (reusing the creator's details)
contact <- list(
  individualName = creator$individualName,
  electronicMailAddress = creator$electronicMailAddress,
  address = address,
  organizationName = "University of Washington"
)

# Create keywords
keywordSet <- list(
  list(
    keywordThesaurus = "ARTIS Project Keywords",
    keyword = list(
      "seafood trade",
      "fisheries",
      "global trade",
      "marine resources",
      "fishery products",
      "seafood traceability",
      "international trade"
    )
  ),
  list(
    keywordThesaurus = "Geographic Terms",
    keyword = list(
      "global",
      "international",
      "trade flows"
    )
  )
)

# Create methods
methods <- list(
  methodStep = list(
    list(
      description = list(
        para = paste(
          "The ARTIS database is created by harmonizing reported seafood trade data from UN Comtrade",
          "and FAO FishStat. Product volumes are converted to live-weight equivalents using conversion factors.",
          "Species are attributed to trade records using the Sea Around Us (SAU) methodology that",
          "allocates trade to likely source countries and species based on production data.",
          "The database contains detailed information on seafood trade with attributes for species,",
          "habitat type, production method, and source country determination."
        )
      )
    )
  )
)

# Create abstract
abstract <- list(
  para = paste(
    "The ARTIS (Aquatic Resource Trade in Species) database provides detailed information on global seafood trade",
    "with attribution to source countries and species. This dataset includes trade flows, product weights, and",
    "live-weight equivalents for seafood products traded internationally. Species, habitat, and production method",
    "information is assigned to trade records using the Sea Around Us (SAU) methodology. The data covers trade from",
    "2002 onwards and includes all countries reporting to UN Comtrade. This particular version (v1.2.0) includes",
    "improvements in species attribution and source country identification."
  )
)

# Define intellectual rights
intellectualRights <- "This dataset is released under CC BY 4.0 International license. Users are free to copy and redistribute the material in any medium or format, and to remix, transform, and build upon the material for any purpose, even commercially, provided appropriate credit is given."

# Create title
title <- "ARTIS v1.2.0 Seafood Trade Data with Sea Around Us Attribution"

# Create pubDate
pubDate <- "2025-12-01"

# Create dataset element
dataset <- list(
  title = title,
  creator = list(creator),
  associatedParty = associatedParty,
  pubDate = pubDate,
  abstract = abstract,
  intellectualRights = intellectualRights,
  keywordSet = keywordSet,
  coverage = coverage,
  contact = contact,
  publisher = publisher,
  methods = methods,
  dataTable = list(dataTable)
)

# Create the EML document
eml_doc <- list(
  packageId = uuid::UUIDgenerate(),
  system = "uuid",
  dataset = dataset
)

# Write the EML document to a file
write_eml(eml_doc, "ARTIS_v1.2.0_trade_SAU_metadata.xml")