#' Pull INT forecast vintage dates

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'adhoc-get-vintage-calcs'
EF_DIR = Sys.getenv('EF_DIR')

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
library(roll)

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
hist = list()

## Load Data ----------------------------------------------------------
local({
	
	# variable_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_variables'))
	forecast_values = as_tibble(dbGetQuery(
		db,
		"SELECT forecast, vdate, freq, varname, date, value
		FROM forecast_values
		WHERE form = 'd1'
		AND forecast ='int'
		AND freq = 'm'"
	))
	
	int_varnames = unique(forecast_values$varname)
	
	# Select unique date-level data
	hist_values = as_tibble(dbGetQuery(
		db,
		"SELECT latest_hist_vdate, first_hist_vdate, freq, date, varname, latest_hist_value
		FROM
		(
			SELECT 
				freq, form, date, varname, MAX(vdate) as latest_hist_vdate,
				last(value, vdate) as latest_hist_value, -- Get value corresponding to latest vdate for each date
				MIN(vdate) AS first_hist_vdate
			FROM forecast_hist_values
			WHERE 
				form = 'd1'
				--AND varname = ANY('{ffr}'::VARCHAR[])
			GROUP BY varname, form, freq, date
			ORDER BY varname, form, freq, date
		) AS a"
	)) %>%
		filter(., varname %in% int_varnames)
	
	int_varnames <<- int_varnames
	forecast_values <<- forecast_values
	hist_values <<- hist_values
})



# Analyze Vintages ----------------------------------------------------------

## Graph Vintage Dates ----------------------------------------------------------
local({

	all_vdates =
		forecast_values %>%
		group_by(., varname) %>%
		mutate(., varname_index = cur_group_id()) %>%
		ungroup(.) %>%
		group_by(., varname, varname_index, vdate) %>%
		summarize(
			.,
			count = n(),
			.groups = 'drop'
		)

	
	all_vdates %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = varname_index, color = varname))
	
	
	

})

