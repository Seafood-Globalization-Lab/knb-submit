# Submit ARTIS to KNB Data Repository

### Purpose

-   Long-term stable archive of model inputs, model, database, and metadata
-   Open-access distribution point for the public and collaborators

ARTIS uses the The Knowledge Network for Biocomplexity [KNB](https://knb.ecoinformatics.org/) data repository to archive and distribute stable releases of the model codebase and resulting database. Archiving, documenting and openly distributing ARTIS is a critical component in contributing to the larger open-science and reproducible science community. ARTIS uses KNB as an access point for anyone to download the [ARTIS model codebase and clean inputs](https://doi.org/10.5063/F1862DXT) and output [ARTIS database](https://doi.org/10.5063/F1CZ35N7)

KNB is guided by [FAIR](https://doi.org/10.1038/sdata.2016.18) (findable, accessible, interoperable, resuble) principles of data sharing and preservation and issues unique DOIs (digital object identifier) to each data package and every version of the package for long term access, transparency, and informative citations. KNB is a member of [DataONE](https://www.dataone.org/) (Data Observation Network for Earth); a network of data repositories. KNB uses [EML](https://eml.ecoinformatics.org/) (Ecological Metadata Language) to document objects within a data packages and can be authored via the website GUI (graphical user interface) or through a series of R packages; the ARTIS pipeline uses [`EMLassemblyline`](https://github.com/EDIorg/emlAssemblyLine)

### Additional KNB resources
 For how to submit data to KNB see the links below from NCEAS (National Center for Ecological Analysis and Synthesis) who develops and maintains DataONE and KNB.

- [KNB and ADC Data Team Training](https://nceas.github.io/datateam-training/training/) - For creating a data package to submit to KNB and editing of existing EML documentation. 
- [Instructions for the EML assembly line](https://nrm.dfg.ca.gov/FileHandler.ashx?DocumentID=197025) - Practical instructions for running the `EMLassemblyline` (EAL) workflow to author EML. 

# Creating EML Metadata for ARTIS Dataset Releases

This guide walks through generating an EML (Ecological Metadata Language) metadata document for a new ARTIS dataset release on the [KNB data repository](https://knb.ecoinformatics.org/). The workflow uses the [`EMLassemblyline`](https://github.com/EDIorg/EMLassemblyline) R package (EAL) combined with custom post-processing scripts to produce valid EML for the ARTIS parquet file collection.

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

The workflow reads the ARTIS parquet dataset from a local path set in your `.Renviron` file. Set this once per machine:

```r
usethis::edit_r_environ(scope = "project")
```

Add this line to the `.Renviron` file that opens, replacing the path with your local ARTIS dataset location:

```bash
ARTIS_DB_PATH=/path/to/your/local/ARTIS/KNB/outputs
```

Save and restart R. Verify it worked:

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

> [!WARNING] 
> **Do not use special characters, symbols, or formatting.** EML only accepts Unicode plain text: UTF-8. URLs are acceptable. The `run_EMLassemblyline_for_metadata-files.R` script trys to reads in `.txt` safely because excel introduces all kinds of crazy things without the user knowing. LLM generated text may also use non-UTF-8 characters that will introduce EML validation problems. 

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
> Open dictionary files in a spreadsheet editor if edits are needed. The `sanitize_encoding()` function in the run script automatically cleans up any encoding artifacts introduced by Excel on save — this is expected and handled.

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
testthat::test_file("tests/testthat/test-artis-eml-validation.R")
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

### Publish to KNB (AM has not check that this is actually correct)

1. Log into [KNB](https://knb.ecoinformatics.org/) with your ORCiD
2. Work in the **staging environment first** to preview and evaluate your submission before going live
3. Reserve a package identifier: **Tools → Reservations → Data Package Identifier Reservations**
4. Update `package.id` in the `make_eml()` call with the reserved identifier (e.g. `"knb.XXXXX.1"`) and re-run the script to regenerate the EML with the correct package ID
5. Upload: **Tools → Evaluate/Upload Data Packages** → select your EML file → manually upload parquet files → click **Evaluate**
6. Review the evaluation report — you can publish with warnings but not with errors
7. Once clean, repeat in the **production environment** with a new reserved identifier
8. Click **Upload** (not Evaluate) to go live and receive a DOI

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