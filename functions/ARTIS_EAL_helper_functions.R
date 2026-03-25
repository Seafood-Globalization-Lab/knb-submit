library(arrow)
library(dplyr)

# Helper: map Arrow type strings to EML class
help_arrow_to_eml_class <- function(type_str) {
  dplyr::case_when(
    type_str %in% c("string", "utf8", "large_utf8") ~ "categorical",
    type_str %in% c("int32", "int64", "uint32", "uint64", "int16", "int8") ~ "numeric",
    type_str %in% c("double", "float", "float32", "float64") ~ "numeric",
    type_str %in% c("date32", "date64") ~ "Date",
    type_str %in% c("timestamp[ms]", "timestamp[us]", "timestamp[s]") ~ "Date",
    .default = "character"
  )
}

# Helper: build and write an EML attribute template from an Arrow schema
help_write_eml_attributes <- function(data_table, out_path) {
  
  field_names <- schema(data_table)$names
  field_types <- sapply(seq_along(field_names), \(i) schema(data_table)[[i]]$type$ToString())
  eml_classes <- help_arrow_to_eml_class(field_types)
  
  # follow template structure provided by EMLassemblyline
  attr_df <- tibble::tibble(
    attributeName            = field_names,
    attributeDefinition      = "",
    class                    = eml_classes,
    unit                     = dplyr::if_else(eml_classes == "numeric", "!Add units here!", ""),
    dateTimeFormatString     = dplyr::if_else(eml_classes == "Date", "!Add datetime specifier here!", ""),
    missingValueCode         = "",
    missingValueCodeExplanation = ""
  )
  
  write.table(
    attr_df,
    file      = out_path,
    sep       = "\t",
    row.names = FALSE,
    quote     = FALSE,
    na        = ""
  )
  
  message("Written: ", out_path)
}


# create representative subset of parquet data in csv --------------------

get_parquet_data_subset <- function(path_data_dir) {
  paths_parquet <- list.files(
    file.path(path_data_dir, "reference_tables"),
    full.names = TRUE
  ) %>%
    c(
      # only want single representative trade file for metadata
      list.files(
        file.path(path_data_dir, "trade", "HS96"),
        full.names = TRUE
      )[1]
    ) %>%
    c(
      # only want single representative consumption file metadata
      list.files(
        file.path(path_data_dir, "consumption", "HS96"),
        full.names = TRUE
      )[1]
    )
  return(paths_parquet)
}

convert_artis_to_csv <- function(paths_artis_parquet, path_write_csv){
    # go through each path in the supplied vector of file paths
  purrr::walk(paths_artis_parquet, \(a_file_path){
    # read in parquet file to a table
    a_table <- arrow::read_parquet(a_file_path)

    # get file name without file extension 
    a_file_name <- tools::file_path_sans_ext(basename(a_file_path))

    # adjust trade and consumption file names to make generic for metadata
    if(stringr::str_detect(string = a_file_name, pattern = "trade")) {
      # remove hs version and year specific from file name
      a_file_name <- stringr::str_replace(string = a_file_name, pattern = "_HS\\d+_\\d+", replacement = "")
    } else if(stringr::str_detect(string = a_file_name, pattern = "consumption")) {
            # remove hs version and year specific from file name
      a_file_name <- stringr::str_replace(string = a_file_name, pattern = "_HS\\d+_\\d+", replacement = "")
    }

    a_file_name_csv <- paste0(a_file_name, ".csv")

    # write table as a csv in the designated directory
    arrow::write_csv_arrow(a_table, file.path(path_write_csv, a_file_name_csv))

  })
}
  
