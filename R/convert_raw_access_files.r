library(tidyverse)
library(arrow)
library(future)
library(purrr)
library(furrr)
library(glue)

plan(multisession, workers = 8)

access_files <- list.files('data/historical/raw', pattern = '(mdb$|accdb$|ACC$)', full.names = TRUE) %>%
  tibble(f = .) %>%
  mutate(year = str_extract(f, '[0-9]{4}')) %>%
  filter(as.integer(year) > 2023)

future_map(
  1:nrow(access_files),
  function(row){
    year <- access_files[row,]$year
    accdb <-  access_files[row,]$f
    dir.create(file.path('data/historical/extracts', year), recursive = TRUE)
    tbls <- system(glue('/opt/homebrew/bin/mdb-tables "{accdb}"'), intern = TRUE) %>% 
      str_split_1(pattern = '\\s+') %>%
      str_subset('.+')
    for(tbl in tbls){
      tbl_fmt <- str_replace(tbl, '^[0-9]+_', '') # remove table numbers from 2021+ exports
      system(glue('/opt/homebrew/bin/mdb-export "{accdb}" "{tbl}" > data/historical/extracts/{year}/{tbl_fmt}.csv'))
      tryCatch(read_csv(glue('data/historical/extracts/{year}/{tbl_fmt}.csv')) %>% write_parquet(glue('data/historical/extracts/{year}/{tbl_fmt}.parquet')), error = function(e) message(e))
    }
  }
)

