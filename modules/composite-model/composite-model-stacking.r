#' This gets historical data for website

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-stacking'
EF_DIR = Sys.getenv('EF_DIR')
IMPORT_DATE_START = '2007-01-01'

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(readxl)
library(httr)
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
releases = list()
hist = list()

## Load Variable Defs ----------------------------------------------------------
variable_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_variables'))
release_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_hist_releases'))


# Import Test Backdates ----------------------------------------------------------

## Import Historical Data ----------------------------------------------------------
hist_data = tbl(db, sql(
	"SELECT varname, vdate, date, value
	FROM forecast_hist_values
	WHERE varname = 'gdp' AND freq = 'q'AND form = 'd1'"
	)) %>%
	collect(.) %>%
	# Only keep initial release for each historical value
	# This results in date being the unique row identifier
	group_by(., varname, date) %>%
	filter(., vdate == min(vdate)) %>%
	ungroup(.) %>%
	arrange(., varname, date)

## Import Forecasts ----------------------------------------------------------
forecast_data = tbl(db, sql(
	"SELECT varname, forecast, vdate, date, value
	FROM forecast_values
	WHERE varname = 'gdp' AND freq = 'q' AND form = 'd1'"
	)) %>%
	collect(.)

## Get Available Dates ----------------------------------------------------------
vintage_values =
	merge(
		as.data.table(forecast_data),
		rename(as.data.table(hist_data), hist_vdate = vdate, hist_value = value),
		by = c('varname', 'date'),
		all = FALSE,
		allow.cartesian = TRUE
		) %>%
	.[, c('error', 'days_before_release') := list(hist_value - value, as.numeric(hist_vdate - vdate))] %>%
	.[days_before_release >= 1] %>%
	# .[, c('hist_vdate', 'hist_value') := NULL] %>%
	# Only keep initial release for each historical value
	# For each obs date, get the forecast & vdates
	split(., by = c('date', 'varname')) %>%
	lapply(., function(x)
		x %>%
			dcast(., days_before_release ~ forecast, value.var = 'value') %>%
			merge(
				data.table(days_before_release = seq(max(.$days_before_release), 1, -1)),
				.,
				by = 'days_before_release',
				all = TRUE
				) %>%
			.[order(-days_before_release)] %>%
			.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
			melt(., id.vars = 'days_before_release', value.name = 'value', variable.name = 'forecast', na.rm = T) %>%
			.[,
			  c('varname', 'date', 'hist_vdate', 'hist_value') :=
			  	list(x$varname[[1]], x$date[[1]], x$hist_vdate[[1]], x$hist_value[[1]])
			  ]
		) %>%
	rbindlist(.)

vintage_values_input =
	vintage_values %>%
	dcast(., varname + date + hist_vdate + hist_value + days_before_release ~ forecast, value.var = 'value')

vintage_values_input %>% filter(., date == max(date)) %>% view(.)


test_data =
	anti_join(
		forecast_data,
		rename(hist_data, hist_vdate = vdate, hist_value = value),
		by = c('varname', 'date')
		) %>%
	filter(., date >= '2010-01-01') %>%
	filter(., date == min(date)) %>%
	pivot_longer(., )

llrf = ll_regression_forest(
	X = vintage_values[, c('days_before_release', 'forecast', 'value')] %>% replace(., is.na(.), Inf) %>% as.matrix(.),
	Y = vintage_values[, c('hist_value')] %>% replace(., is.na(.), Inf) %>% as.matrix(.),
	enable.ll.split = TRUE
	)
