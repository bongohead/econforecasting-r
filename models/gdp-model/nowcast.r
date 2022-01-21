#'  Run this script on scheduler after close of business each day

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'GDP_NOWCAST'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = F

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(DIR, 'logs', 'nowcast.log'), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(paste0('Run ', Sys.Date()))
}

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)

## Load Connection Info ----------------------------------------------------------
source(file.path(DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)
releases = list()
hist = list()
ext = list()

## Import SQL Data & Transform ----------------------------------------------------------
local({
	
	hist_data = tbl(db, sql(
		"SELECT h.* FROM variable_params p
		LEFT JOIN historical_values h
			ON p.varname = h.varname
		WHERE
			p.nc_dfm_input = TRUE AND
			transform = 'base'"
		)) %>%
		
	
	
	
})
