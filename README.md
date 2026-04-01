# Submit ARTIS to data repository

## Purpose

-   Long-term stable archive of model inpusts, model, database, and metadata
-   Open-acess distribution point

ARTIS uses the The Knowledge Network for Biocomplexity [KNB](https://knb.ecoinformatics.org/) data repository to archive and distribute stable releases of the model codebase and resulting database. Archiving, documenting and openly distributing ARTIS is a critical component in contributing to the larger open-science and reproducible science community. ARTIS uses KNB as an access point for anyone to download the [ARTIS model codebase and clean inputs](https://doi.org/10.5063/F1862DXT) and output [ARTIS database](https://doi.org/10.5063/F1CZ35N7)

KNB is guided by [FAIR](https://doi.org/10.1038/sdata.2016.18) (findable, accessible, interoperable, resuble) principles of data sharing and preservation and issues unique DOIs (digital object identifier) to each data package and every version of the package for long term access, transparency, and informative citations.

## Further details & resources

KNB is a member of [DataONE](https://www.dataone.org/) (Data Observation Network for Earth); a network of data repositories. KNB uses [EML](https://eml.ecoinformatics.org/) (Ecological Metadata Language) to document objects within a data packages which can be created via the website GUI (graphical user interface) or through a series of R packages [rdataone](http://doi.org/10.5063/F1M61H5X) and [arcticdatautils](https://github.com/NCEAS/arcticdatautils/).

**Note**: a user will need an [ORCiD](https://orcid.org/) to log into KNB.

For additional resources on how to submit data to KNB see the links below from NCEAS (National Center for Ecological Analysis and Synthesis) who develops and maintains DataONE and KNB.

-   [KNB and ADC Data Team Training](https://nceas.github.io/datateam-training/training/)
-   [Data Team Reference Guide](https://nceas.github.io/datateam-training/reference/)

## Creating Metadata for ARTIS

This repo contains scripts to semi-automatically create EML formatted metadata to accompany the data submission to the KNB data repository. \
\
FIXIT: Add more instructions about running these scripts and what the specific files do. \
\

### Draft/Outline Instructions

- Remove / `.gitignore` `.metadata/attributers_*.txt` and `.metdata/catvars_*.txt` files. 
    - Can easily be recreated in the `run_EMLassemblyline_for_metadata-files.R`
- Keep `.metadata/abstract.md`, `.metadata/additional_info.md`, `.metadata/intellectual_rights.txt`, `.metadata/keywords.txt`, `.metadata/methods.md`, and `.metadata/personnel.txt`.
    - Contain values that should be carried over to next release version. 
- `EMLassemblyline` does not work for parquet files. Added workaround in `run_EMLassemblyline_for_metadata-files.R` (`EAL`) to convert a sample of ARTIS KNB files into .csv to run through the `EAL` workflow to generate first valid iteration of the EML doc. Probably need some "post-processing" EML.xml edits to correct things like EML `<physical>` elements. 

### AM notes about things:

-   Missing values `""` for `categorical` class data attributes don't seem to validate when calling `EMLassemblyline::make_eml()`. I retained the definitions in `artis_data_dictionary_attributes_catvars.txt` but will assign `NA` as the missing value
- `personnel.txt` `role` column does not take EML specific values for the [`ResearchProjectType/personnel/role`](https://eml.ecoinformatics.org/schema/eml-project_xsd#ResearchProjectType_ResearchProjectType_personnel_role). It seems like `EMLassemblyline::make_eml()` translates the valid values outlined in [EAL DIY instructions](file:///Users/theamarks/Downloads/DIY%20Instructions%20EMLassemblyLine%20in%20R_2023-11.pdf) and translates them to valid EML `ResearchProjectType/personnel/role` values. 
- 

### Post `EMLassemblyline::make_eml()` Fixes

- `<physical>` reads from `.csv` files in `./metadata-files/ data_objects/` - Might need work around to accurately represent `.parquet` files


## Claude README Documentation Draft 2026-03-31



