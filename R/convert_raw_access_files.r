library(tidyverse)
library(arrow)
library(future)
library(purrr)

plan(multisession, workers = 8)

access_files <- list.files('data/raw', pattern = '(mdb$|accdb$|ACC$)') %>%
  tibble(f = .) %>%
  mutate(year = str_extract(f, '[0-9]{4}')) %>%
  filter(as.integer(year) > 2020)

future_map(
  1:nrow(access_files),
  function(row){
    year <- access_files[row,]$year
    accdb <-  access_files[row,]$f
    dir.create(file.path('~/sandbox/ics209plus/data', year), recursive = TRUE)
    tbls <- system(glue('mdb-tables "{accdb}"'), intern = TRUE) %>% 
      str_split_1(pattern = '\\s+') %>%
      str_subset('.+')
    for(tbl in tbls){
      tbl_fmt <- str_replace(tbl, '^[0-9]+_', '') # remove table numbers from 2021+ exports
      system(glue('mdb-export "{accdb}" "{tbl}" > ~/sandbox/ics209plus/data/{year}/{tbl_fmt}.csv'))
      tryCatch(read_csv(glue('~/sandbox/ics209plus/data/{year}/{tbl_fmt}.csv')) %>% write_parquet(glue('~/sandbox/ics209plus/data/{year}/{tbl_fmt}.parquet')), error = function(e) message(e))
    }
  }
)

