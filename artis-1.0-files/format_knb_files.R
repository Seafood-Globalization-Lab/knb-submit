# Format data for KNB upload
# i.e., break up artis and consumption files by year-hs version

# Load packages
library(tidyverse)
library(DBI)

# Initial database pulls

# Database connection
con <- dbConnect(RPostgres::Postgres(),
                 dbname=Sys.getenv("DB_NAME"),
                 host="localhost",
                 port="5432",
                 user=Sys.getenv("DB_USERNAME"),
                 password=Sys.getenv("DB_PASSWORD"))

# Check that connection is established by checking which tables are present
dbListTables(con)

# ARTIS dataframe
artis <- dbGetQuery(con, "SELECT * FROM snet") %>%
  select(-record_id) %>%
  mutate(hs6 = case_when(
    str_length(hs6) == 5 ~ paste("0", hs6, sep = ""),
    TRUE ~ hs6
  )) 

# Create dataframe of year-hs combinations
year_hs <- artis %>%
  select(hs_version, year) %>%
  distinct() %>%
  arrange(hs_version, year)

# Loop through each year and version combination to write it out
for(i in 1:nrow(year_hs)){
  print(paste(i, year_hs$hs_version[i], year_hs$year[i], sep = " "))
  
  artis_i <- artis %>%
    filter(hs_version == year_hs$hs_version[i], 
           year == year_hs$year[i])
  
  write.csv(artis_i, file.path("outputs", "trade", paste("artis_midpoint_", 
                                                         year_hs$hs_version[i], "_",
                                                         year_hs$year[i], ".csv",
                                                         sep = "")),
            row.names = FALSE)
}


# consumption dataframe
consumption <- dbGetQuery(con, "SELECT * FROM consumption") %>%
  select(-record_id) 

