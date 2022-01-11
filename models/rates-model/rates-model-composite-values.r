#'  Run this script on scheduler after rates-model-submodel.r of each day

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'rates_model_composite'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = F

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log')), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(paste0('Run ', Sys.Date()))
}

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)
library(highcharter)

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


# Get SQL Data ----------------------------------------------------------

## Historical Data ----------------------------------------------------------
hist_data =
	dbGetQuery(db, 'SELECT * FROM rates_model_hist_values') %>% 
	as_tibble(.)

## Submodel Data ----------------------------------------------------------
submodel_data =
	dbGetQuery(db, 'SELECT * FROM rates_model_submodel_values') %>% 
	as_tibble(.)

## Submodel Data Interpolated  ----------------------------------------------------------
#' This fills in submodel data for vintage dates that are missing 
#' 
local({
	
	submodel_data %>%
		# Split into submodel-forecasted date groups
		group_split(., submodel, varname, freq, date) %>% 
		purrr::imap(., function(x, i) {
			message(i)
			freq = switch(x$freq[[1]], 'd' = 'day', 'm' = 'month', 'q' = 'quarter')
			tibble(vdate = seq(min(x$vdate), ceiling_date(x$date[[1]], freq) - days(1), '1 day')) %>%
				left_join(., transmute(x, vdate, value), by = 'vdate') %>%
				mutate(., value = zoo::na.locf(value))
			}) %>% 
		.[[5]]
	
	#' Note: need to add max allowable break:
	#' E.g., suppose one submodel-date combination forecasts for 2020Q1 from 2010Q1
	#' This forecast should "expire" at a certain point, even if no new data is released!
	
})


# Stacked Models ----------------------------------------------------------

## Inflation  ----------------------------------------------------------
local({
	
	
	
})


## FFR ----------------------------------------------------------
local({
	
	
	# 
	
	# Assume date of historical vintage is the same as the date of actual vintage
	error_df =
		submodel_data %>%
		filter(., varname == 'ffr') %>%
		inner_join(
			.,
			hist_data %>%
				filter(., varname == 'ffr' & freq == 'm') %>%
				transmute(., date, actual = value) %>%
				arrange(., date) %>%
				mutate(., lag_actual = lag(actual)),
			by = 'date'
		) %>%
		# The release date for non-daily data is pushed to end-of-period values
		group_split(., freq) %>%
		purrr::map_dfr(., function(x) 
			x %>%
				mutate(
					.,
					day_before_value = interval(vdate, ceiling_date(
						date,
						{if (x$freq[[1]] == 'm') 'month' else if (x$freq[[1]] == 'q') 'q' else stop()}
						) - days(1)) %/% days(1)
					)
			) %>%
		# Forecast error
		mutate(
			.,
			forecast_error = abs((actual - value)/lag_actual)
			)
	
	error_df %>%
		group_by(., vdate) %>%
		summarize(
			.,
			obs_pred = n(),
			mae = mean(abs(forecast_error)),
			.groups = 'drop'
		) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = mae))
	
	error_df %>%
		group_by(., submodel, freq, dates_before_release) %>%
		summarize(., mae = mean(abs(forecast_error)), n = n(), .groups = 'drop') %>%
		filter(., n >= 3) %>%
		ggplot(.) +
		geom_line(aes(x = -1 * dates_before_release, y = mae, color = submodel))

})

# Scenario Forecasts ----------------------------------------------------------
