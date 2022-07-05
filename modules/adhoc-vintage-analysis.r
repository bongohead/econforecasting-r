# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
EF_DIR = Sys.getenv('EF_DIR')

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)

## Load Connection Info ----------------------------------------------------------
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)


# Get Data ----------------------------------------------------------
as_tibble(dbGetQuery(db, str_glue(
	"SELECT
		vdate, date, value
	FROM forecast_values
	WHERE forecast = 'int'
		AND freq = 'm'
		AND varname = 'ffr'
		AND form = 'd1'"
	))) %>%
	pivot_wider(., id_cols = 'vdate', names_from = 'date', values_from = 'value') %>%
	View(.)

	