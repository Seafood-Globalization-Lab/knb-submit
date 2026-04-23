{    
  library(dataone)
  library(datapack)
  library(digest)
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
