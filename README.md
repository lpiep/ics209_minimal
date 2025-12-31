# Harmonize 1999 - Present ICS209 Report Data

This project extracts selected information from ICS209 Reports, past and present, including
three "historical" file formats, and the current API specification. It runs regularly to 
keep the final data up-to-date. 

## Output

### Files

* `data/current/current_cleaned.parquet`: Latest version of data pulled from the API, plus any fires that 
previously appeared in the API export. 
* `data/historical/historical_cleaned.parquet`: Data extracted from historical archives. Not expected to change.

### Variables

The output data set draws from different variables depending on the year of the original data. 

| Output Variable | Description | 1999-2002 | 2003-2013 | 2013-2024 | 2025+ (current_cleaned.parquet) |
| --- | --- | --- | --- | --- | --- |
| `ics_state` |  US State in which fire occurred | `UN_USTATE` | `UN_USTATE` (except 2013, where `OWNERSHIP_STATE` used) | `POO_STATE_CODE` | `STATE` |
| `ics_county` |  US County in which fire occurred | `COUNTY` | `COUNTY` | `POO_COUNTY_CODE` | `POO_COUNTY` |
| `ics_wildfire_area` |  Burned area in square kilometers | `ACRES` | `AREA` & `AREA_MEASUREMENT` | `CURR_INCIDENT_AREA` | `DISP_INC_AREA` |
| `ics_complex` | Whether fire is a complex of multiple member fires | -- | `COMPLEX` | `SINGLE_COMPLEX_FLAG` | `COMPLEX_FLAG` |
| `ics_name` |  Fire name | `ENAME` | `INCIDENT_NAME` | `INCIDENT_NAME` | `INCIDENT_NAME` |
| `ics_wildfire_fatalities_civ` | Number of civilian fatalities | -- | -- | `QTY_TO_DATE` & `CIT_IDENTIFIER`  | -- |
| `ics_wildfire_fatalities_tot` | Number of total fatalities | `FATALITIES` | `FATALITIES` | `QTY_TO_DATE` & `CIT_IDENTIFIER` | `FATALITIES` |
| `ics_wildfire_injuries_civ` | Number of civilian injuries or illnesses | -- | -- | `QTY_TO_DATE` & `CIT_IDENTIFIER`  | -- |
| `ics_wildfire_injuries_tot` | Number of total injuries or illnesses | `INJURIES` (2001, 2002 only) | `INJURIES_TO_DATE` | `QTY_TO_DATE` & `CIT_IDENTIFIER` | `INJURIES_TO_DATE` & `INJURIES_THIS_REP_PERIOD`|
| `ics_wildfire_evacuated_civ` | Number of people evacuated | -- | -- | `QTY_TO_DATE` & `CIT_IDENTIFIER` | -- |
| `ics_wildfire_evacuated_tot` | Number of people evacuated | -- | -- | `QTY_TO_DATE` & `CIT_IDENTIFIER` | -- |
| `ics_wildfire_struct_destroyed` | Number of structures destroyed | `DCOUNT` | `DESTROYED` | `QTY_DESTROYED` | `STRUCTURES_DESTROYED_COUNT` |
| `ics_wildfire_struct_threatened` | Number of structures threatened | `TCOUNT` | `THREATENED` | `QTY_THREATENED` | `STRUCTURES_THREATENED_COUNT` |
| `ics_wildfire_cost` | Estimated final cost of response | `EST_FINAL_COSTS` | `EST_FINAL_COSTS` | `PROJECTED_FINAL_IM_COST` | `PROJECTED_FINAL_IM_COST` |
| `ics_wildfire_ignition_date` | Date of fire ignition | `STARTDATE` | `START_DATE` | `DISCOVERY_DATE` | `DISCOVERY_DATE` |
| `ics_wildfire_poo_lat` | Fire point of origin latitude | `LATDEG` & `LATMIN` | `LATITUDE` | `POO_LATITUDE` | `POO_LATITUDE` |
| `ics_wildfire_poo_lon` | Fire point of origin longitude | `LONGDEG` & `LONGMIN` | `LONGITUDE` | `POO_LONGITUDE` | `POO_LONGITUDE` |
| `ics_id` | Year of Start date , Native ID of associated ICS/209 data, Incident Name (separated by '_') | `INCIDENT_NUMBER` & `STARTDATE` &  `ENAME` | `INCIDENT_NUMBER` & `START_DATE` & `INCIDENT_NAME` | `INCIDENT_NUMBER` & `DISCOVERY_DATE` & `INCIDENT_NAME`  |  `INCIDENT_NUMBER` & `DISCOVERY_DATE` & `INCIDENT_NAME` |
| `ics_irwin_id` |  | -- | -- | `IRWIN_IDENTIFIER` | -- |



## Current

For current year data, you'll need to obtain approval to access the FAMWEB database from USFS IT. I contacted the contact for FAMWEB listed on [wildfire.gov](https://www.wildfire.gov/contact-us). 

You can access current year summaries at https://famdwh-dev.nwcg.gov/sit209/cognos_report_queries/sit209_data_report.
Ask your contact at USFS IT to provide the current credentials, or contact [me](mailto:loganap@uw.edu) for more info.

This repository runs the query regularly using github actions, and should update any fires that change in subsequent queries.


## Historical

ISC209s are forms submitted to the Federal Government to describe emergencies. St. Dennis et al have 
published more comprehensive code to clean and harmonize historical ICS209 data (their project is called ICS209-PLUS), 
but only publish the resulting data through 2020. To recreate and extend their procedure, we need to 
obtain the raw ICS209 summaries from USFS. The current year and historical summaries are kept separately.

1999 - 2023 are available as MS Access EXEs here: https://www.wildfire.gov/application/sit209. They are also included as 
gz files in this repo. When run, each 
file will open a popup asking where to save the access db files. Unfortunately, it does not appear to be 
simple to automate the extraction of these files, but their date will be made available here and kept up to
date

This has already been done, with cleaned data in `data/historical`. However, if you need to recreate it, you can continue.

### Extracting with WINE

Don't have access to a Windows machine to run EXE files? Use [WINE](https://www.winehq.org/) on Linux or OSX.

For each file, you'll need to run `$ wine the_file.exe` at the terminal and extract the files to the "Z:\\" 
drive, which points to your actual root directory (as opposed to the "C:\\Documents" folder in WINE, which
is part of the Windows Emulator (I know, WINE Is Not an Emulator, but whatever). 

Grab a cup of coffee, put on a podcast, and do that for a bit. Next you can install `mdbtools` to deal with the
`mdb` files. The `Hmisc` package has a nice `mdb.get` function that uses that library to pull everything into R. 

### Processing

First run `convert_raw_access_files.r`, which extracts each of the `accdb` and `mdb` databases to Parquet files (one 
file per table). 

Next, parse_historical extracts data for each of the three historical data formats found in these files. 

### Known Issues

* 1999-2001 did not contain fatality data
* The following are not available before 2014:
       * evacuations
       * civilian fatalities
       * civilian injuries
* The linking code used to identify fatalities, injuries, and evacuations were not available in the 2016 data set
* The Access DB for 2021 did not include the main file used to join each table to the primary fire information. USFS-IT 
provided this file directly, and it is included in the final data set and archived raw files here. 

### Reference

https://training.fema.gov/emiweb/is/icsresource/assets/ics%20forms/ics%20form%20209,%20incident%20status%20summary%20(v3).pdf
