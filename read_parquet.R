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

















































