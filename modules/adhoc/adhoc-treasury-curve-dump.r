#' Ad-hoc request to get Treasury curves in CSV form on website

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
EF_DIR = Sys.getenv('EF_DIR')

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(jsonlite)
library(DBI)
library(econforecasting)

## Load Connection Info ----------------------------------------------------------
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))

# Load All Data ----------------------------------------------------------
forecast_data = as_tibble(dbGetQuery(db, sql(
	"SELECT
		vdate, varname, date, d1 AS forecast_value
	FROM
	forecast_values_v2_all
	WHERE
		forecast = 'int'
		AND varname ~ '^t\\d\\d[y|m]$'
		AND freq = 'm'
	"
)))

hist_data = as_tibble(dbGetQuery(db, sql(
	"SELECT
		varname, date, d1 AS hist_value
	FROM forecast_hist_values_v2_latest
	WHERE
		varname ~ '^t\\d\\d[y|m]$'
		AND freq = 'm'
	"
)))


joined_data =
	forecast_data %>%
	left_join(., hist_data, by = c('date', 'varname')) %>%
	transmute(., date_of_forecast = vdate, forecast_month = date, variable = varname, forecast_value, hist_value) %>%
	arrange(., date_of_forecast, forecast_month) %>%
	filter(., year(date_of_forecast) %in% 2021 & month(date_of_forecast) %in% 12)


#%>%
	# pivot_wider(., id_cols = c('vdate', 'date'), names_from = 'varname', values_from = 'value') %>%
	# transmute(., date_updated = vdate, date_forecasted = date, t01m, t03m, t06m, t01y, t02y, t05y, t10y, t20y, t30y)

# Load All Vintage Data ----------------------------------------------------------

# data %>% data.table::fwrite(., '/var/www/econforecasting.com/public/static/data/treasury_forecasts_all.csv')
