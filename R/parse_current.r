#########################################
### Update Current Year Data from API ###
#########################################

options(scipen = 999)

library(tidyverse)
library(glue)
library(httr)
library(fs)
library(jsonlite)
library(arrow)

download_event_ics209_raw <- function(dst){
  fs::dir_create(path_dir(dst))
  httr::GET(
    url = 'https://famdwh-dev.nwcg.gov/sit209/cognos_report_queries/sit209_data_report',
    authenticate(Sys.getenv('FAMDWH_USR'), Sys.getenv('FAMDWH_PW'), type = "basic"), # non-secret authentication
    invisible(httr::write_disk(path = dst, overwrite = TRUE))
  )
  dst
}

read_ics209_raw <- function(f){
  ics_tabular <- read_json(f) %>%
    map(discard, is.null) %>% 
    map(as_tibble) %>% 
    bind_rows()
}

download_event_ics209_raw('data/current/current_raw')

# Get latest submitted report for each file
curr_data <- read_ics209_raw('data/current/current_raw') %>% 
  group_by(INCIDENT_NAME, INCIDENT_NUMBER) %>% 
  arrange(SUBMITTED_DATE) %>%
  slice_tail(n = 1) %>%
  ungroup() 

stopifnot(all(curr_data$DISP_INC_AREA_UNIT == 'Acres')) # Confirm all areas in acres

parse_lat_long <- function(l){
  l <- str_split_fixed(l, n=4, pattern = '[^\\d]+')
  as.numeric(l[,1]) + as.numeric(l[,2])/60 + as.numeric(l[,3])/60/60
}

curr_data_cleaned <- curr_data %>% 
  transmute(
    ics_id = INCIDENT_NUMBER,
    ics_wildfire_ignition_date =  mdy(DISCOVERY_DATE),
    ics_wildfire_fatalities_total = FATALITIES, # not found
    ics_name = INCIDENT_NAME,
    ics_wildfire_area = as.numeric(DISP_INC_AREA) * 0.00404686, # convert to km^2
    ics_wildfire_struct_destroyed = STRUCTURES_DESTROYED_COUNT,
    ics_wildfire_poo_lat = parse_lat_long(curr_data$POO_LATITUDE),
    ics_wildfire_poo_lon = parse_lat_long(curr_data$POO_LONGITUDE),
    ics_state = STATE,
    ics_county = POO_COUNTY,
    ics_complex = if_else(COMPLEX_FLAG == 'X', TRUE, FALSE)
  )

# add in previously downloaded data, updating where needed
if(file_exists('data/current/current_cleaned.parquet')){
  previous_data <- read_parquet('data/current/current_cleaned.parquet')
  curr_data_cleaned <- bind_rows(
    curr_data_cleaned,
    anti_join(previous_data, curr_data_cleaned, by = 'ics_id')
  )
}

write_parquet(curr_data_cleaned, 'data/current/current_cleaned.parquet')

