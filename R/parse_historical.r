#####################################################
### Extract Vars of Interest from Historical Data ###
#####################################################

library(arrow)
library(tidyverse)


# 1999 to 2002
dat <- map(dir_ls('data/historical/extracts/', recurse = TRUE, regexp = '(1999|2000|2001|2002)/.*parquet'), read_parquet)
z <- map(dat, names) %>%
  enframe() %>%
  unnest(value) 

dat_99_02 <- list(
  incident_informations = dat %>%
    keep_at(~ str_detect(.x, 'IMSR_INCIDENT_INFORMATIONS.parquet')) %>% 
    map(select, c(EVENT_ID, STARTDATE, ENAME, ACRES, LATDEG, LATMIN, LONGDEG, LONGMIN, REPDATE, UN_USTATE)) %>% 
    bind_rows() %>%
    rename(INCIDENT_NUMBER = EVENT_ID) %>%
    group_by(INCIDENT_NUMBER) %>% 
    arrange(REPDATE) %>% 
    slice_tail(n=1) %>% 
    ungroup(),

  incident_structures = dat %>% 
    keep_at(~ str_detect(.x, 'IMSR_INCIDENT_STRUCTURES.parquet')) %>% 
    map(select, c(II_EVENT_ID, DCOUNT, II_REPDATE)) %>%
    bind_rows() %>%
    rename(INCIDENT_NUMBER = II_EVENT_ID) %>% 
    group_by(INCIDENT_NUMBER) %>% 
    arrange(II_REPDATE) %>% 
    slice_tail(n=1) %>%
    ungroup(),
  
  incidents = dat %>% 
    keep_at(~ str_detect(.x, 'IMSR_IMSR_209_INCIDENTS.parquet')) %>% 
    map(select, c(INCIDENT_NUMBER, FATALITIES, REPORT_DATE, COUNTY)) %>%
    bind_rows() %>% 
    group_by(INCIDENT_NUMBER) %>% 
    arrange(REPORT_DATE) %>% 
    slice_tail(n=1) %>%
    ungroup()
) %>%
  reduce(full_join, by = 'INCIDENT_NUMBER') %>%
  mutate( # remove minutes of lat/long outside range
    LATMIN = if_else(LATMIN >= 60, NA_real_, LATMIN),
    LONGMIN = if_else(LONGMIN >= 60, NA_real_, LONGMIN),
  ) %>% 
  transmute(
    ics_id = INCIDENT_NUMBER,
    ics_wildfire_ignition_date = as_date(mdy_hms(STARTDATE, quiet = TRUE)),
    ics_wildfire_fatalities = FATALITIES,
    ics_name = ENAME,
    ics_wildfire_area = ACRES * 0.00404686,
    ics_wildfire_struct_destroyed = DCOUNT,
    ics_wildfire_poo_lat = LATDEG + LATMIN/60,
    ics_wildfire_poo_lon = -abs(LONGDEG) - (LONGMIN/60), # assume positive longs are shorthand for negative
    ics_state = UN_USTATE,
    ics_county = COUNTY,
    ics_complex = NA # not found
  ) %>%
  distinct() %>%
  filter(ics_wildfire_ignition_date < ymd('2003-01-01')) # remove dates in the future


# 2002 to 2013
dat <- map(dir_ls('data/historical/extracts/', recurse = TRUE, regexp = '(200[2-9]|201[0-3])/.*parquet'), read_parquet)

dat_02_13 <- list(
  incident_informations = dat %>%
    keep_at(~ str_detect(.x, 'IMSR_IMSR_209_INCIDENTS(_T)?.parquet')) %>% 
    map(
      select, 
      any_of(c('INCIDENT_NUMBER', 'FATALITIES', 'START_DATE', 'INCIDENT_NAME', 'AREA', 
               'AREA_MEASUREMENT', 'LATITUDE', 'LONGITUDE', 'REPORT_DATE', 'UN_USTATE', 
               'COUNTY', 'COMPLEX'))
    ) %>% 
    bind_rows() %>% 
    group_by(INCIDENT_NUMBER) %>% 
    arrange(REPORT_DATE) %>% 
    slice_tail(n=1) %>%
    ungroup() %>%
    select(-REPORT_DATE),
  
  incident_structures = dat %>% 
    keep_at(~ str_detect(.x, 'IMSR_IMSR_209_INCIDENT_STRUCTURES.parquet')) %>% 
    map(select, c(IM_INCIDENT_NUMBER, IM_REPORT_DATE, DESTROYED)) %>%
    bind_rows() %>%
    rename(INCIDENT_NUMBER = IM_INCIDENT_NUMBER) %>%
    group_by(INCIDENT_NUMBER) %>% 
    arrange(IM_REPORT_DATE) %>% 
    slice_tail(n=1) %>%
    ungroup() %>%
    select(-IM_REPORT_DATE)
  
) %>%
  reduce(full_join, by = 'INCIDENT_NUMBER') %>%
  transmute(
    ics_id = INCIDENT_NUMBER,
    ics_wildfire_ignition_date = as_date(mdy_hms(START_DATE, quiet = TRUE)),
    ics_wildfire_fatalities = FATALITIES,
    ics_name = INCIDENT_NAME,
    ics_wildfire_area = case_when(
      AREA_MEASUREMENT == 'ACRES' ~ AREA * 0.00404686,
      AREA_MEASUREMENT == 'HECTARES' ~ AREA * 0.01,
      AREA_MEASUREMENT == 'SQ MILES' ~ AREA * 2.58999,
      AREA_MEASUREMENT == 'SQ KM' ~ AREA,
      TRUE ~ NA_real_
    ),
    ics_wildfire_struct_destroyed = DESTROYED,
    ics_wildfire_poo_lat = LATITUDE,
    ics_wildfire_poo_lon = -abs(LONGITUDE), # assume positive longs are shorthand for negative
    ics_state = UN_USTATE,
    ics_county = COUNTY,
    ics_complex = case_when(
      COMPLEX == 'Y' ~ TRUE,
      COMPLEX == 'N' ~ FALSE
    )
  ) %>%
  distinct() %>%   
  filter(ics_wildfire_ignition_date < ymd('2014-01-01')) # remove dates in the future

  
# 2013 - pres
# need to add SIT209_HISTORY_INCIDENT_209_CSLTY_ILLNESSES_
# need to figure out how that and structures join to history_incidents table (seems to be via INC209R_IDENTIFIER)
fips_codes <- read_parquet('data/fips_codes.parquet')
fips_codes_states <- fips_codes %>% select(-matches('county')) %>% distinct()
fips_codes_counties <- fips_codes %>% select(-state_name, -state) %>% distinct()

dat <- map(dir_ls('data/historical/extracts/', recurse = TRUE, regexp = '(202[0-9]|201[3-9])/.*parquet'), read_parquet)

# Need to extract the code for fatalities since it is different for every year!
fatality_code <- dat %>%
  keep_at(~ str_detect(.x, 'SIT209_HISTORY_SIT209_LOOKUP_CODES.parquet')) %>%
  map(filter, CODE_TYPE == 'CASUALTY_ILLNESS_TYPE' & CODE_NAME == 'Fatalities') %>% 
  map(select, 'LUCODES_IDENTIFIER') %>%
  bind_rows(.id = 'data_year') %>%
  mutate(data_year = str_extract(data_year, '(?<=data/historical/extracts/)[0-9]{4}'))

# Get INCIDENT_NUMBER <-> IRWINID
irwin_xwalk = dat %>%
  keep_at(~ str_detect(.x, 'SIT209_HISTORY_INCIDENTS.parquet')) %>% 
  map(
    select, 
    c(INCIDENT_NUMBER,
      IRWIN_IDENTIFIER)
  ) %>% 
  map(mutate, INCIDENT_NUMBER = str_replace(as.character(INCIDENT_NUMBER), '^0+', '')) %>% 
  bind_rows() %>% 
  mutate(INCIDENT_NUMBER = str_replace(as.character(INCIDENT_NUMBER), '^0+', '')) %>% 
  group_by(INCIDENT_NUMBER) %>% 
  filter(!is.na(IRWIN_IDENTIFIER)) %>% 
  filter(n() == 1) %>% # definite links only
  ungroup()

dat_13_plus <- list(
  incident_reports = dat %>%
    keep_at(~ str_detect(.x, 'SIT209_HISTORY_INCIDENT_209_REPORTS.parquet')) %>% 
    map(
      select, 
      c(INCIDENT_NUMBER,
        #INCIDENT_IDENTIFIER,
        INC209R_IDENTIFIER,
        INCIDENT_NAME,
        CURR_INCIDENT_AREA,
        DISCOVERY_DATE,
        POO_LATITUDE,
        POO_LONGITUDE,
        POO_STATE_CODE,
        POO_COUNTY_CODE,
        SINGLE_COMPLEX_FLAG,
        LAST_MODIFIED_DATE)#,
        #IRWIN_IDENTIFIER)
    ) %>% 
    map(
      mutate, 
      INCIDENT_NUMBER = str_replace(as.character(INCIDENT_NUMBER), '^0+', ''),
      POO_STATE_CODE = as.numeric(POO_STATE_CODE), 
      POO_COUNTY_CODE = as.numeric(POO_COUNTY_CODE)
    ) %>% 
    bind_rows() %>% 
    group_by(INCIDENT_NUMBER) %>% 
    arrange(LAST_MODIFIED_DATE) %>% 
    slice_tail(n=1) %>%
    ungroup() %>%
    select(-LAST_MODIFIED_DATE),
  
  incident_structures = dat %>% 
    keep_at(~ str_detect(.x, 'SIT209_HISTORY_INCIDENT_209_AFFECTED_STRUCTS.parquet')) %>% 
    map(select, c(INC209R_IDENTIFIER, QTY_DESTROYED)) %>%
    bind_rows() %>%
    filter(!is.na(QTY_DESTROYED)) %>% 
    group_by(INC209R_IDENTIFIER) %>% 
    arrange(QTY_DESTROYED) %>% 
    slice_tail(n=1) %>%
    ungroup(),
  
  incident_deaths = dat %>% 
    keep_at(~ str_detect(.x, 'SIT209_HISTORY_INCIDENT_209_CSLTY_ILLNESSES.parquet')) %>% 
    map(select, c(INC209R_IDENTIFIER, CIT_IDENTIFIER, RESPONDER_PUBLIC_FLAG, QTY_TO_DATE)) %>%
    bind_rows(.id = 'data_year') %>%
    mutate(data_year = str_extract(data_year, '(?<=data/historical/extracts/)[0-9]{4}')) %>%
    left_join(fatality_code, by = 'data_year') %>% 
    filter(CIT_IDENTIFIER == LUCODES_IDENTIFIER & RESPONDER_PUBLIC_FLAG == 'P') %>% # public deaths
    group_by(INC209R_IDENTIFIER) %>% 
    arrange(QTY_TO_DATE) %>% 
    slice_tail(n=1) %>%
    ungroup() 
) %>%
  reduce(full_join, by = 'INC209R_IDENTIFIER') %>% 
  left_join(fips_codes_states, by = c('POO_STATE_CODE' = 'state_code')) %>% #translate fips codes to state/county
  left_join(fips_codes_counties, by = c('POO_STATE_CODE' = 'state_code', 'POO_COUNTY_CODE' = 'county_code')) %>%
  left_join(irwin_xwalk, by = 'INCIDENT_NUMBER') %>% 
  transmute(
    ics_id = INCIDENT_NUMBER,
    #ics_irwin_id = IRWIN_IDENTIFIER,
    ics_wildfire_ignition_date = as_date(mdy_hms(DISCOVERY_DATE, quiet = TRUE)),
    ics_wildfire_fatalities = QTY_TO_DATE, # not found
    ics_name = INCIDENT_NAME,
    ics_wildfire_area = CURR_INCIDENT_AREA * 0.00404686, # acres to km2
    ics_wildfire_struct_destroyed = QTY_DESTROYED,
    ics_wildfire_poo_lat = POO_LATITUDE,
    ics_wildfire_poo_lon = -abs(POO_LONGITUDE), # assume positive longs are shorthand for negative
    ics_state = state,
    ics_county = county,
    ics_complex = case_when(
      SINGLE_COMPLEX_FLAG == 'C' ~ TRUE,
      SINGLE_COMPLEX_FLAG == 'S' ~ FALSE
    ),
    ics_irwin_id = IRWIN_IDENTIFIER
  ) %>%
  distinct() %>%   
  filter(ics_wildfire_ignition_date < today()) # remove dates in the future

# Join and Clean 

dat <- bind_rows(
  dat_99_02,
  dat_02_13,
  dat_13_plus
) %>%
  mutate( 
    ics_wildfire_poo_lat = if_else(ics_wildfire_poo_lat == 0 | ics_wildfire_poo_lon == 0, NA_real_, ics_wildfire_poo_lat),
    ics_wildfire_poo_lon = if_else(ics_wildfire_poo_lat == 0 | ics_wildfire_poo_lon == 0, NA_real_, ics_wildfire_poo_lon),
    ics_wildfire_area = if_else(ics_wildfire_area == 0, NA_real_, ics_wildfire_area)
  ) %>%
  filter(!is.na(ics_id) & ics_id != '') 

write_parquet(dat, 'data/historical/historical_cleaned.parquet')
