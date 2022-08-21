#' This gets historical data for website

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-stacking'
EF_DIR = Sys.getenv('EF_DIR')
TRAIN_VDATE_START = '2010-01-01'

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
library(xgboost)

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


# Load Data ----------------------------------------------------------
hist_data = tbl(db, sql(
	"SELECT val.varname, val.vdate, val.date, val.value
	FROM forecast_hist_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE form = 'd1'"
	)) %>%
	collect(.) %>%
	# Only keep initial release for each historical value
	# This results in date being the unique row identifier
	group_by(., varname, date) %>%
	filter(., vdate == min(vdate)) %>%
	ungroup(.) %>%
	arrange(., varname, date)

# Only get forecasts data for when date is < ahead of vdate
forecast_data = tbl(db, sql(
	"SELECT val.varname, val.forecast, val.vdate, val.date, val.value
	FROM forecast_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE form = 'd1'
		AND date <= vdate + INTERVAL '12 MONTHS' AND date >= vdate + INTERVAL '6 MONTHS'"
	)) %>%
	collect(.)

forecast_data_1m_3m = tbl(db, sql(
	"SELECT val.varname, val.forecast, val.vdate, val.date, val.value
	FROM forecast_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE form = 'd1'
		AND date <= vdate + INTERVAL '3 MONTHS' AND date >= vdate + INTERVAL '1 MONTHS'"
)) %>%
collect(.)

forecast_data_3m_1y = tbl(db, sql(
	"SELECT val.varname, val.forecast, val.vdate, val.date, val.value
	FROM forecast_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE form = 'd1'
		AND date <= vdate + INTERVAL '12 MONTHS' AND date >= vdate + INTERVAL '3 MONTHS'"
)) %>%
collect(.)



joined_data =
	forecast_data %>%
	inner_join(
		.,
		transmute(hist_data, varname, date, hist = value),
		by = c('date', 'varname')
	) %>%
	mutate(
		.,
		year_vdate = year(vdate),
		error = abs(hist - value)
		)


performance_data =
	joined_data %>%
	group_by(varname, forecast, year_vdate) %>%
	na.omit(.) %>%
	summarize(., mae = mean(error), n = n(), .groups = 'drop') %>%
	filter(., n >= 4) %>%
	group_split(., varname) %>%
	set_names(., map_chr(., ~ .$varname[[1]])) %>%
	lapply(., function(x)
		x %>%
			pivot_wider(., id_cols = forecast, names_from = year_vdate, values_from = mae)
	)



joined_data %>%
	group_by(varname, forecast, date) %>%
	na.omit(.) %>%
	summarize(., mae = mean(error), n = n(), .groups = 'drop') %>%
	filter(., n >= 4) %>%
	group_split(., varname) %>%
	set_names(., map_chr(., ~ .$varname[[1]])) %>%
	lapply(., function(x)
		x %>%
			pivot_wider(., id_cols = forecast, names_from = date, values_from = mae)
	) %>%
	.$gdp


