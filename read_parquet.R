library(googledrive)
library(arrow)
library(dplyr)
library(purrr)

library(jsonlite)
library(httr)
library(googledrive)



# List parquet files in folder


cat(sprintf("Found %d parquet files\n", nrow(files)))

# Download and read each parquet file
dfs <- files %>%
  pull(id) %>%
  map(function(file_id) {
    tmp <- tempfile(fileext = ".parquet")
    drive_download(as_id(file_id), path = tmp, overwrite = TRUE)
    read_parquet(tmp)
  })

combined <- bind_rows(dfs)

cat(sprintf("Total rows: %d, unique events: %d\n", nrow(combined), n_distinct(combined$event_id)))



library(googledrive)

drive_find(pattern = "batch", type = "application/octet-stream")

library(arrow)

tmp <- tempfile(fileext = ".parquet")
drive_download(drive_find(pattern = "batch.parquet"), path = tmp, overwrite = TRUE)
df <- read_parquet(tmp)


library(tidyverse)
files <- drive_ls(as_id(folder_id), pattern = "batch*")

#---------------------------------------------------------

library(googledrive)
library(dplyr)
library(purrr)

recent_file <- drive_find(
  pattern = "batch",
  n_max = 5,                     
  order_by = "modifiedTime desc" 
) %>% 
  mutate(
    date = map_chr(drive_resource, "modifiedTime"),
    size = map_chr(drive_resource, "size", .default = NA_character_) 
  ) %>% 
  select(-drive_resource) %>% 
  head(1) %>% 
  pull(name)

tmp <- tempfile(fileext = ".parquet")
drive_download(drive_find(pattern = "batch.parquet", n_max = 1), path = tmp, overwrite = TRUE)
df_drive <- arrow::read_parquet(tmp)


df_drive %>% 
  group_by(event_id, league_name, away_team, home_team) %>%
  summarise(side = first(line))


sides = function(df, which_side){
  op_name <- paste0(which_side, "_ol")
  cl_name <- paste0(which_side, "_cl")
  df %>% 
    mutate(
      timestamp = as.POSIXct(timestamp, origin = "1970-01-01", tz = "Europe/Berlin")
    ) %>% 
    filter(side == which_side, market_type == 'moneyline') %>%
    filter(!is.na(odds)) %>% 
    group_by(event_id) %>% 
    mutate(
      mvmnt = row_number()
    ) %>%
    arrange(timestamp, .by_group = TRUE) %>% 
    summarise(
      hme =first(na.omit(home_team)),
      awy =first(na.omit(away_team)),
      cmp =first(na.omit(league_name)),
      op_odds = first(na.omit(odds)),
      cl_line = last(na.omit(odds)),
      mvmnt =  max(mvmnt),
      .groups = "drop" 
    ) %>% 
    arrange(event_id) %>% 
    rename(
      !!op_name := op_odds,
      !!cl_name := cl_line
    )
}



sides(df_drive, 'home') %>% 
  inner_join(sides(df_drive, 'away'), by = 'event_id')


# hme = first(na.omit(home_team)),
# awy = first(na.omit(away_team)),
# comp = first(na.omit(league_name)),
# hme_op    = first(na.omit(home_price)),
# home_cp   = last(na.omit(home_price)),
# away_op   = first(na.omit(away_price)),
# away_cp   = last(na.omit(away_price)),
# total_mve = last(na.omit(mvmnt)),


library(dplyr)
library(tidyr)

home = df_drive %>% 
  filter(side == 'home',
         market_type == 'moneyline',
         event_id == 1628954395) %>% 
  select(event_id, sport_id, league_id, league_name, home_team, away_team,
         starts, period, line, max_limit, timestamp, odds) %>%
  rename(hme_odds = odds)

# 2. Keep 'away' clean to avoid duplicate metadata columns during join
away = df_drive %>% 
  filter(side == 'away', 
         market_type == 'moneyline',
         event_id == 1628954395) %>% 
  select(event_id, timestamp, odds) %>%
  rename(awy_odds = odds)

# 3. Keep 'draw' clean
draw = df_drive %>% 
  filter(side == 'draw', 
         market_type == 'moneyline',
         event_id == 1628954395) %>% 
  select(event_id, timestamp, odds) %>%
  rename(draw_odds = odds)

full_timeline <- home %>% 
  full_join(away, by = c('event_id', 'timestamp')) %>% 
  full_join(draw, by = c('event_id', 'timestamp')) %>% 
  arrange(timestamp) %>% 
  group_by(event_id) %>% 
  fill(
    sport_id, league_id, league_name, home_team, away_team, 
    starts, period, line, max_limit, 
    hme_odds, awy_odds, draw_odds, 
    .direction = "downup"
  ) %>% 
  ungroup() %>% 
  mutate(mvmnt = row_number(),
         timestamp = as.POSIXct(timestamp, 
                                origin = "1970-01-01", 
                                tz = "Europe/Berlin")) 


full_timeline %>%
  mutate(
    home_raw = round((100/(((100/hme_odds) + (100/draw_odds) + (100/awy_odds)))*100)/hme_odds, 2),
    draw_raw = round((100/(((100/hme_odds) + (100/draw_odds) + (100/awy_odds)))*100)/draw_odds, 2),
    away_raw = round((100/(((100/hme_odds) + (100/draw_odds) + (100/awy_odds)))*100)/awy_odds, 2)
  ) %>% group_by(event_id) %>%
  mutate(
    home_diff = round(abs(home_raw - first(home_raw)),2),
    draw_diff = round(abs(draw_raw - first(draw_raw)),2),
    away_diff = round(abs(away_raw - first(away_raw)),2)
  ) %>% 
  select(home_raw, draw_raw, away_raw,
         home_diff,draw_diff, away_diff)



# =============================================================================
# parquet.R — Drive data source functions
# =============================================================================
# This file provides the data pipeline for reading and transforming
# batch.parquet data from Google Drive. It is source()'d from server.R.
#
# Assumes tidyverse packages (dplyr, tidyr, lubridate) are already loaded.
# =============================================================================


# -----------------------------------------------------------------------------
# read_drive_parquet()
# -----------------------------------------------------------------------------
# Stub function that returns an empty data frame with the correct Batch_Parquet
# schema. Replace the body with actual Google Drive API logic when ready.
#
# Returns: data.frame with columns matching the batch.parquet schema
# -----------------------------------------------------------------------------
read_drive_parquet <- function() {


  # TODO: Implement Google Drive API authentication here

  # ---------------------------------------------------------------
  # 1. Authenticate using OAuth credentials (see core_files/credentials.json)
  #    - Use googledrive::drive_auth() or gargle for token management
  #    - Store/refresh tokens in core_files/token.json
  #
  # 2. Download batch.parquet from the configured Google Drive folder
  #    - Use googledrive::drive_download() to fetch the file
  #    - Target file: "batch.parquet" in the shared Drive folder
  #

  # 3. Read the downloaded parquet file
  #    - Use arrow::read_parquet() to load into a data frame
  #    - Example: raw_df <- arrow::read_parquet("batch.parquet")
  #
  # 4. Return the raw data frame
  # ---------------------------------------------------------------

  # STUB: Returns an empty data frame with the correct column structure
  sample_data <- data.frame(
    event_id    = integer(),
    sport_id    = integer(),
    league_id   = integer(),
    league_name = character(),
    home_team   = character(),
    away_team   = character(),
    starts      = character(),
    period      = integer(),
    market_type = character(),
    line        = numeric(),
    side        = character(),
    timestamp   = character(),
    odds        = numeric(),
    max_limit   = numeric(),
    stringsAsFactors = FALSE
  )

  return(sample_data)
}


# -----------------------------------------------------------------------------
# pivot_parquet_to_wide()
# -----------------------------------------------------------------------------
# Takes a long-format parquet dataframe with a `side` column (values: "home",
# "draw", "away") and pivots to wide format with separate columns for each
# side's odds. Aligns rows by event_id and timestamp.
#
# Args:
#   long_df: data.frame with columns including event_id, timestamp, side, odds
#
# Returns: data.frame with columns: event_id, league_name, home_team,
#          away_team, starts, timestamp, home_price, draw_price, away_price,
#          max_limit
# -----------------------------------------------------------------------------
pivot_parquet_to_wide <- function(long_df) {

  if (nrow(long_df) == 0) {
    return(
      data.frame(
        event_id    = integer(),
        league_name = character(),
        home_team   = character(),
        away_team   = character(),
        starts      = character(),
        timestamp   = character(),
        home_price  = numeric(),
        draw_price  = numeric(),
        away_price  = numeric(),
        max_limit   = numeric(),
        stringsAsFactors = FALSE
      )
    )
  }

  wide_df <- long_df %>%
    select(event_id, league_name, home_team, away_team, starts,
           timestamp, side, odds, max_limit) %>%
    pivot_wider(
      id_cols      = c(event_id, league_name, home_team, away_team,
                       starts, timestamp, max_limit),
      names_from   = side,
      values_from  = odds,
      values_fn    = list(odds = first)
    ) %>%
    rename(
      home_price = home,
      draw_price = draw,
      away_price = away
    )

  return(wide_df)
}


# -----------------------------------------------------------------------------
# compute_probabilities()
# -----------------------------------------------------------------------------
# Shared normalization function that transforms raw parquet data (long format)
# into the same processed structure as getAllDatafromDB() output.
#
# Steps:
#   1. Filter to moneyline records only
#   2. Pivot from long to wide format (home_price, draw_price, away_price)
#   3. Apply normalization formula to compute raw probabilities
#   4. Compute diffs from opening odds per event
#   5. Add movement row number per event group
#
# Args:
#   raw_df: data.frame from read_drive_parquet() with Batch_Parquet schema
#
# Returns: data.frame matching the df structure from getAllDatafromDB():
#          event_id, league_name, home_team, away_team, starts, pulled_at,
#          home_price, draw_price, away_price, home_diff, draw_diff,
#          away_diff, mvmnt
# -----------------------------------------------------------------------------
compute_probabilities <- function(raw_df) {

  if (nrow(raw_df) == 0) {
    return(
      data.frame(
        event_id    = integer(),
        league_name = character(),
        home_team   = character(),
        away_team   = character(),
        starts      = as.POSIXct(character()),
        pulled_at   = as.POSIXct(character()),
        home_price  = numeric(),
        draw_price  = numeric(),
        away_price  = numeric(),
        home_diff   = numeric(),
        draw_diff   = numeric(),
        away_diff   = numeric(),
        mvmnt       = integer(),
        stringsAsFactors = FALSE
      )
    )
  }

  # 1. Filter to moneyline records only
  moneyline_df <- raw_df %>%
    filter(market_type == "moneyline")

  if (nrow(moneyline_df) == 0) {
    return(
      data.frame(
        event_id    = integer(),
        league_name = character(),
        home_team   = character(),
        away_team   = character(),
        starts      = as.POSIXct(character()),
        pulled_at   = as.POSIXct(character()),
        home_price  = numeric(),
        draw_price  = numeric(),
        away_price  = numeric(),
        home_diff   = numeric(),
        draw_diff   = numeric(),
        away_diff   = numeric(),
        mvmnt       = integer(),
        stringsAsFactors = FALSE
      )
    )
  }

  # 2. Pivot from long to wide format
  wide_df <- pivot_parquet_to_wide(moneyline_df)

  # 3. Apply normalization formula and compute diffs
  result <- wide_df %>%
    arrange(event_id, timestamp) %>%
    mutate(
      home_raw = round(
        (100 / (((100 / home_price) + (100 / draw_price) + (100 / away_price))) * 100) / home_price, 2
      ),
      draw_raw = round(
        (100 / (((100 / home_price) + (100 / draw_price) + (100 / away_price))) * 100) / draw_price, 2
      ),
      away_raw = round(
        (100 / (((100 / home_price) + (100 / draw_price) + (100 / away_price))) * 100) / away_price, 2
      )
    ) %>%
    # 4. Compute diffs from opening odds per event
    group_by(event_id) %>%
    mutate(
      home_diff = round(abs(home_raw - first(home_raw)), 2),
      draw_diff = round(abs(draw_raw - first(draw_raw)), 2),
      away_diff = round(abs(away_raw - first(away_raw)), 2)
    ) %>%
    # 5. Convert timestamps and add movement counter
    mutate(
      starts = floor_date(ymd_hms(starts, tz = 'Europe/Berlin'), 'minute'),
      pulled_at = floor_date(ymd_hms(timestamp, tz = 'Europe/Berlin'), 'minute'),
      mvmnt = row_number()
    ) %>%
    ungroup() %>%
    # Select final columns matching getAllDatafromDB() output
    select(
      event_id,
      league_name,
      home_team,
      away_team,
      starts,
      pulled_at,
      home_price,
      draw_price,
      away_price,
      home_diff,
      draw_diff,
      away_diff,
      mvmnt
    )

  return(result)
}














































