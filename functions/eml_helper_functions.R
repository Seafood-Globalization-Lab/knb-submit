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
