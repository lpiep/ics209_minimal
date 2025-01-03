# Harmonize 1999 - Present ICS209 Report Data

This project extracts selected information from ICS209 Reports, past and present, including
three "historical" file formats, and the current API specification. It runs regularly to 
keep the final data up-to-date. 

## Output

### Files

* `data/current/current_cleaned.parquet`: Latest version of data pulled from the API, plus any fires that 
previously appeared in the API export. 
* `data/historical/historical_cleaned.parquet`: Data extracted from historical archives. Not expected to change.

### Format

* "ics_id": ID found in original data set ("INCIDENT_NUMBER")
* "ics_wildfire_ignition_date": Start date or first report date of fire
* "ics_wildfire_fatalities": Number of Fatalities (civilian-only when available)
* "ics_name": Fire name
* "ics_wildfire_area": Fire burn area, in square kilometers
* "ics_wildfire_struct_destroyed": Number of structures destroyed
* "ics_wildfire_poo_lat": Latitude of Point of Origin in decimal degrees
* "ics_wildfire_poo_lon": Latitude of Point of Origin in decimal degrees         
* "ics_state": Point of origin USPS state abbreviation (either sic. or derived from FIPS code)
* "ics_county": Point of origin county (either sic. or derived from FIPS code)
* "ics_complex": Whether row represents a complex
* "ics_irwin_id": IRWIN ID when available


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
* 2021 did not include the main file used to join each table to the primary fire information. No fires from this year were
included (though a couple with a start date in 2021 were included in the 2022 data set). 



