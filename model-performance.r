# Analyze historical performance


# Initialize ---------------------------------------------------------

## Set Constants ---------------------------------------------------------
DIR = Sys.getenv('EF_DIR')
DL_DIR = file.path(Sys.getenv('EF_DIR'), 'tmp')
INPUT_DIR = file.path(Sys.getenv('EF_DIR'), 'model-inputs') # Path to directory with constants.r (SQL DB info, SFTP info, etc.)
OUTPUT_DIR = file.path(Sys.getenv('EF_DIR'), 'model-outputs')
VINTAGE_DATE_START = as.Date('2010-01-01')
VINTAGE_DATE_END = Sys.Date()

## Load Libs ---------------------------------------------------------
library(tidyverse)
library(data.table)
library(jsonlite)
library(lubridate)
library(httr)
library(rvest)
library(DBI)
library(econforecasting)

## Load Credentials ---------------------------------------------------------
source(file.path(INPUT_DIR, 'constants.r'))

## Initiate Model Object ---------------------------------------------------------
m = list()
class(m) = 'model'
m$vdates = seq(VINTAGE_DATE_START, VINTAGE_DATE_END, by = '1 day')

## DB Conn ---------------------------------------------------------
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
	)




# Pull Data ---------------------------------------------------------

## Variable Params ---------------------------------------------------------
local({
	
	variables = readxl::read_excel(file.path(INPUT_DIR, 'inputs.xlsx'), sheet = 'all-variables')
	
	variables <<- variables
})

## Pull External Forecast Vintage ---------------------------------------------------------
local({
	
	# Get last vintage for each (tskey, varname)
	external_forecasts =
		DBI::dbGetQuery(db, 'SELECT * FROM ext_tsvalues') %>%
		as_tibble(.) %>%
		group_by(., tskey, varname) %>%
		mutate(., vdate_last = max(vdate)) %>%
		filter(., vdate == vdate_last) %>%
		ungroup(.)
	
	
	vdates_by_variable =
		external_forecasts %>%
		group_by(., varname) %>%
		summarize(., max_vdate = max(vdate), min_vdate = min(vdate))
	
	m$external_forecasts <<- external_forecasts
})



## Pull Historical Data --------------------------------------------------------------------
#' 
#' For any 
#' 
#' 
local({
	
	fredRes =
		variables %>%
		purrr::transpose(.)
		purrr::keep(., ~ .$source == 'fred') %>%
		lapply(., function(x) {
			
			message('Getting data ... ', x$varname)
			
			# Get series data
			dataDf =
				get_fred_data(
					x$sckey,
					CONST$FRED_API_KEY,
					.freq = x$freq,
					.return_vintages = TRUE,
					.verbose = F
					) %>%
				dplyr::filter(., vintageDate %in% ) %>%
				dplyr::filter(., vintageDate == max(vintageDate)) %>%
				dplyr::select(., -vintageDate) %>%
				dplyr::transmute(., date = obsDate, value) %>%
				dplyr::filter(., date >= as.Date('2010-01-01'))
			
			list(dataDf = dataDf)
		})
	
	
	for (varname in names(fredRes)) {
		p$variables[[varname]]$rawData <<- fredRes[[varname]]$dataDf
	}
	
	
	
	
})


