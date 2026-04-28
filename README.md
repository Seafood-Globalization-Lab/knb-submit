# Publish ARTIS Database on KNB Data Repository

### Purpose

- Reproducible and versioned creation of EML metadata
- Version controled data dictionaries
- Reusable automated upload of database and metadata to KNB

ARTIS uses the The Knowledge Network for Biocomplexity [KNB](https://knb.ecoinformatics.org/) data repository to archive and publically distribute stable releases of the model codebase and database. Archiving, documenting and openly distributing ARTIS is a critical component in contributing to the larger open-science and reproducible science community. 

KNB is guided by [FAIR](https://doi.org/10.1038/sdata.2016.18) (findable, accessible, interoperable, resuble) principles of data sharing and preservation and issues unique DOIs (digital object identifier) to each data package and every version of the package for long term access, transparency, and informative citations. KNB is a member of [DataONE](https://www.dataone.org/) (Data Observation Network for Earth); a network of data repositories and KNB uses [EML](https://eml.ecoinformatics.org/) (Ecological Metadata Language) to document objects within a data packages and can be authored via the website GUI (graphical user interface) or through a series of R packages; the ARTIS workflow uses the [`EMLassemblyline`](https://github.com/EDIorg/EMLassemblyline) R package (EAL) combined with custom post-processing scripts to produce valid EML for the ARTIS parquet file collection.

### References and Resources
- [KNB and ADC Data Team Training](https://nceas.github.io/datateam-training/training/) - For creating a data package to submit to KNB and editing of existing EML documentation. 
- [Instructions for the EML assembly line](https://nrm.dfg.ca.gov/FileHandler.ashx?DocumentID=197025) - Practical instructions for running the `EMLassemblyline` (EAL) workflow to author EML. 
- [DataOne R package documentation](https://dataoneorg.r-universe.dev/dataone) Check out the `Vignettes` particularly:
  - [DataONE Federation](https://dataoneorg.r-universe.dev/articles/dataone/v02-dataone-federation.html) for KNB Authentication Tokens and;
  - [Uploading Datasets to DataONE](https://dataoneorg.r-universe.dev/articles/dataone/v06-update-package.html) for an outline of the data package upload workflow. 

## Installation

In your local IDE terminal: 

```
git clone https://github.com/Seafood-Globalization-Lab/knb-submit.git
```

## Quick Start Instructions

### 1) Prerequisite Assumptions

- ARTIS database is validated 
- Database is in cleaned final architecture on the Seafood Globalization Lab's UW NAS server

### Set ARTIS file path in `.Renviron`

The workflow reads the ARTIS parquet dataset from a local path set in your `.Renviron` file. Set this once per machine:

```r
usethis::edit_r_environ(scope = "project")
```

Add this line to the `.Renviron` file that opens, replacing the path with your local ARTIS dataset location:

```bash
ARTIS_DB_PATH=/path/to/your/local/ARTIS/KNB/outputs
```

Save and restart R.

### 2) Update `config.yml` 

This file contains all the model configuration parameters settings. Update the values for a new version of ARTIS. These values are used dynamically in the workflow scripts. 

### 3) Update `artis_dictionaries/` files (if needed)

These files need updating when any of the following occurs between ARTIS versions: 

- A new HS version is added - `artis_dictionary_hs_version.txt`
- A new column is added - `artis_dictionary_tbl_attributes.txt`
- A new table is added - `artis_dictionary_tbl.txt` (Also requires code changes in `01_*.R`)
- A categorical variable is new or expanded - `artis_dictionary_tbl_attributes_catvars.txt`
- Add a citation to the dataset - `artis_citations.bib`

### 4) Update Persistent EMLassemblyline templates

The `artis_metadata_files/` directory is where the R package `EMLassemblyline` operates to write EML. Update files if needed: 

- `abstract.md`
- `additional_info.md`
- `keywords.txt`
- `methods.md`
- `personnel.txt`

> [!WARNING] 
> **Do not use special characters, symbols, or formatting.** EML only accepts Unicode plain text: UTF-8. URLs are acceptable. 

### 5) Run `1_run_EMLassemblyline_for_artis_metadata_files.R`

likely able to run straight through

### 6) Run `02_publish_ARTIS_data_on_KNB.R`

Requires setting KNB staging node credientials:

1) Navigate to [KNB test/staging site](https://dev.nceas.ucsb.edu/) (may need to use a different browser)
2) Login with your ORCiD
3) Navigate to your profile --> settings --> authentication Token --> "Token for DataONE R" 
4) Copy the string to your clipboad. Paste and call in your console to set temporary access token

> [!Tip]
> if having problems, click small red link on login popup window and adjust browser settings)
Check out [dataone r package vignette](https://cran.r-project.org/web/packages/dataone/vignettes/v02-dataone-federation.html) for more detailed instructions. Note the vignette instructions are for the productions site, we need a stagging site token

### Inspect on KNB test site

### Fixing EML

### Publish with DOI

## Details on using `EMLassemblyline` Metadata

This section documents details of using this repo to author EML and publish on KNB. 

## Relevant File Architecture

```
knb-submit/
├── run_EMLassemblyline_for_metadata-files.R   # Main workflow script — run this
├── functions/
│   └── ARTIS_EAL_helper_functions.R           # Helper functions sourced by run script
├── tests/
│   └── testthat/
│       └── test-artis-eml-validation.R        # Validation tests for dictionaries & templates
└── metadata-files/
    ├── artis_dictionary_tbl_attributes.txt        # ⭐ Attribute definitions (long-lived)
    ├── artis_dictionary_tbl_attributes_catvars.txt # ⭐ Categorical variable definitions (long-lived)
    ├── artis_dictionary_hs_version.txt            # ⭐ HS version definitions (long-lived)
    ├── artis_dictionary_tbl.txt                   # ⭐ Table-level descriptions (long-lived)
    ├── data_objects/                              # Representative .csv files (auto-generated)
    ├── eml/                                       # Output EML .xml files land here
    └── metadata_templates/
        ├── abstract.md                            # ⭐ Dataset abstract (update each release)
        ├── methods.md                             # ⭐ Dataset methods (update if needed)
        ├── additional_info.md                     # ⭐ Additional dataset info
        ├── keywords.txt                           # ⭐ Dataset keywords
        ├── personnel.txt                          # ⭐ Creator/contact/PI info (update each release)
        ├── intellectual_rights.txt                # License (rarely needs editing)
        ├── attributes_*.txt                       # Auto-generated — do NOT manually edit
        └── catvars_*.txt                          # Auto-generated — do NOT manually edit
```

> ⭐ = files you may need to update for a new release. All other files are either auto-generated or rarely change.

## Prerequisites

### Environment setup

 Verify it worked:

```r
Sys.getenv("ARTIS_DB_PATH")
```
> [!NOTE]
> You will need an [ORCiD](https://orcid.org/) to log into KNB. Create one if you don't have one — it also serves as your author identifier in the metadata.

## `EMLassemblyline` Workflow

### Overview

The EML generation workflow has three stages:

1. **Update metadata inputs** — update dictionaries, templates, and config values for the new release
2. **Run the main script** — `EMLassemblyline` generates EML for representative `.csv` files; post-processing replaces `.csv` references with the full parquet file collection
3. **Validate and publish** — run tests, validate EML, upload to KNB

### Update the script config

Open `run_EMLassemblyline_for_metadata-files.R` and update the top config section:

```r
# Personal script config
clean_up_templates <- "yes"   # always "yes" when rerunning to avoid stale templates
convert_parquets   <- "yes"   # set to "yes" if the data schema has changed; "no" otherwise
final_eml_name     <- "ARTIS_v1.2_FAO_parquet.xml"  # update version number here
```

Update the `make_eml()` call further down to reflect the new release:

```r
EMLassemblyline::make_eml(
  dataset.title    = "Aquatic Resource Trade in Species (ARTIS) v1.3 FAO",  # update version
  temporal.coverage = c("1996", "2021"),  # update end year if coverage changed
  ...
)
```

### Update `abstract.md`

Open `metadata-files/metadata_templates/abstract.md` in Positron or Rstudio (not Excel) and update the temporal coverage, species counts, or any other release-specific language.



### Update `personnel.txt`

Open `metadata-files/metadata_templates/personnel.txt` in a spreadsheet editor and verify or update author/contact information.

Key rules for this file:

- **At least one `creator` and one `contact` must be listed** — these are required by EML
- **`userId`** must be the 16-digit ORCiD number only, formatted as `XXXX-XXXX-XXXX-XXXX` — not the full URL
- **Valid `role` values** from the EAL documentation: `creator`, `contact`, `PI`, `metadataProvider`. Any other string is also accepted and will appear as an associated party. Note these are not `EML` valid values, EAL has its own set that gets translated in `make_eml()`.
- If a person has more than one role, duplicate their row with the second role. One row per role.

### Update data dictionaries (only if columns or tables changed)

The four data dictionary files in `metadata-files/` are designed to persist across releases. Only update them if:

- New columns were added to a table → add rows to `artis_dictionary_tbl_attributes.txt`
- Categorical values changed → update `artis_dictionary_tbl_attributes_catvars.txt`
- A new HS version is included → update `artis_dictionary_hs_version.txt`
- A new table was added → add rows to both attribute and table-level dictionaries. (This might be more complicated and require changing the general table listings within the `run_EMLasseblyline_for_metadata-files.R`.


> [!TIP]
> Open dictionary files in a spreadsheet editor if edits are needed. The code in `01_run_EMLassemblyline_for_artis_metadata_files.R` reads in the text files in a way to remove invalid Excel added characters. 

Valid values for key columns in `artis_dictionary_tbl_attributes.txt`:

| Column | Valid values |
|---|---|
| `class` | `numeric`, `categorical`, `character`, `Date` |
| `unit` | Required when `class == "numeric"`. Use `dimensionless` if no units apply. Must be blank for non-numeric. Run `EMLassemblyline::view_unit_dictionary()` to find valid unit names. |
| `dateTimeFormatString` | Required when `class == "Date"`. Use format codes: `YYYY`, `MM`, `DD`, `hh`, `mm`, `ss`. Must be blank for non-Date. |
| `missingValueCode` | One value per attribute (e.g. `NA`). |

### Run the Main Script

In `run_EMLassemblyline_for_metadata-files.R`, set `convert_parquets <- "yes"` to convert new dataset version. Set to `"no"` if only re-running for same dataset.

Run the full script:

```r
source("run_EMLassemblyline_for_metadata-files.R")
```

The script will:

1. Delete stale `attributes_*.txt` and `catvars_*.txt` template files
2. Read in the ARTIS data dictionaries with encoding sanitization
3. Convert representative parquet files to `.csv` (if `convert_parquets == "yes"`)
4. Run `EMLassemblyline` template functions to generate attribute and categorical variable templates
5. Join your dictionary definitions into the templates
6. Call `EMLassemblyline::make_eml()` to produce an initial EML `.xml` file describing the 8 ARTIS representative `.csv` files of the general table types.
7. Post-process the EML: the 8 representative `.csv` `<dataTable>` elements are replaced with `<dataTable>` elements describing the full collection of `n` parquet files. Each parquet file is matched to its ARTIS table type (e.g. `consumption`, `trade`, `reference_hs6`) and cloned from the corresponding representative `<dataTable>` template — preserving the full `<attributeList>` column definitions. Only the `<physical>` (file name, size, format), `<entityName>`, and `<entityDescription>` fields are updated to reflect each individual parquet file. This means the single representative `consumption` and `trade` templates are each stamped across all of their respective partitioned parquet files (split by HS version and year), while each reference table template is cloned once.
8. Write the final EML to `metadata-files/eml/ARTIS_v1.2_FAO_parquet.xml`
9. Validate the EML — you should see `[1] TRUE` with no errors

If validation fails, the error message will point to the invalid section.

### Check Dictionaries and Templates

Before publishing, run the dictionary and template checks. These are **not** a substitute for formal EML validation (which runs automatically at the end of the main script) — they are a supplementary check designed specifically for the ARTIS workflow. Because `EMLassemblyline` uses its own set of valid values that differ from raw EML schema values, these tests verify that the long-lived ARTIS data dictionaries stay aligned with what `EMLassemblyline` expects when it reads the generated `attributes_*.txt` and `catvars_*.txt` template files.

This is particularly useful to run after editing any of the dictionary files before re-running the main script:

```r
testthat::test_file("tests/testthat/test_artis_dictionaries_valid.R")
```

The checks confirm:

- All `class` values in the ARTIS dictionaries and generated attribute templates are valid EAL values (`numeric`, `categorical`, `character`, `Date`)
- Numeric attributes have units; non-numeric attributes do not
- `Date` attributes have a `dateTimeFormatString`; non-Date attributes do not
- All `attributeDefinition` and categorical `definition` fields are non-empty
- No non-UTF-8 encoding artifacts remain in dictionary character columns
- `personnel.txt` contains at least one `creator`, `contact`, `PI`, and `metadataProvider`
- All ORCiD `userId` values are formatted as `XXXX-XXXX-XXXX-XXXX`

Fix any failures in the source dictionary files and re-run the main script before proceeding to publish.

> **Note:** KNB will assign a new package identifier in the production environment — you cannot reuse the staging identifier. Re-run the script one final time with the production identifier before the final upload.

## AM KNB Datapackage Notes for README

- Will create new dataset version on KNB each release. Will NOT use versioned releases to update an existing dataset. 
- [Instructions to get access token](https://nceas.github.io/datateam-training/training/creating-a-data-package.html#upload-a-package) 
  - Log into KNB with ORCiD
  - my profile
  - settings
  - Authentication 
  - Token for DataONE R
  - Copy
  - run in console
- [CRAN dataone pkg vignettes](https://cran.r-project.org/web/packages/dataone/vignettes/v05-upload-data.html)