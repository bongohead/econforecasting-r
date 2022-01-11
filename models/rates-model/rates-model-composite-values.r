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


# Stacked Models ----------------------------------------------------------

## Inflation  ----------------------------------------------------------
local({
	
	
	
})


## FFR ----------------------------------------------------------
local({
	
	# Assume date of historical vintage is the same as the date of actual vintage
	submodel_data %>%
		filter(., varname == 'ffr') %>%
		inner_join(
			.,
			hist_data %>%
				filter(., varname == 'ffr' & freq == 'd') %>%
				transmute(., date, actual = value),
			by = 'date'
		) %>%
		mutate(., dates_before_release = interval(vdate, date) %/% days(1))
	
	

})

# Scenario Forecasts ----------------------------------------------------------
