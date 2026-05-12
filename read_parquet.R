library(googledrive)
library(arrow)
library(dplyr)
library(purrr)

# Authenticate (uses cached token or opens browser)
drive_auth()

# List parquet files in your folder
folder_id <- Sys.getenv("GOOGLE_DRIVE_FOLDER_ID")
files <- drive_ls(as_id(folder_id), pattern = "\\.parquet$")

cat(sprintf("Found %d parquet files\n", nrow(files)))

# Download and read each parquet file into a list of data frames
dfs <- files %>%
  pull(id) %>%
  map(function(file_id) {
    tmp <- tempfile(fileext = ".parquet")
    drive_download(as_id(file_id), path = tmp, overwrite = TRUE)
    read_parquet(tmp)
  })

# Combine into one data frame
combined <- bind_rows(dfs)

cat(sprintf("Total rows: %d, unique events: %d\n", nrow(combined), n_distinct(combined$event_id)))
