# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-atlnow-stlnow'
EF_DIR = Sys.getenv('EF_DIR')

## Log Job ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(jsonlite)
library(httr)
library(rvest)
library(DBI)
library(econforecasting)
library(lubridate)

## Load Connection Info ----------------------------------------------------------
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
run_id = log_start_in_db(db, JOB_NAME, 'external-import')


# Import ------------------------------------------------------------------


## Get GDP Release Dates ---------------------------------------------------
hist_data =
	collect(tbl(db, sql(
		"SELECT val.varname, val.vdate, val.date, val.value
		FROM forecast_hist_values val
		INNER JOIN forecast_variables v ON v.varname = val.varname
		WHERE form = 'd1' AND  v.varname = 'gdp'"
		))) %>%
	group_by(., date) %>%
	{inner_join(
		filter(., vdate == min(vdate)) %>% transmute(., date, first_vdate = vdate, first_value = value),
		filter(., vdate == max(vdate)) %>% transmute(., date, final_vdate = vdate, final_value = value),
		by = 'date'
		)} %>%
	ungroup(.) %>%
	arrange(., date)

## Get ATL + STL Forecasts ---------------------------------------------------
local({

	api_key = get_secret('FRED_API_KEY', file.path(EF_DIR, 'model-inputs', 'constants.yaml'))

	fred_sources = tribble(
		~ forecast, ~ varname, ~ fred_key,
		'atlnow', 'gdp','GDPNOW',
		'atlnow', 'pce', 'PCENOW',
		'stlnow', 'gdp', 'STLENI'
	)

	fred_data =
		fred_sources %>%
		purrr::transpose(.) %>%
		lapply(., function(x) {
			get_fred_data(x$fred_key, api_key, .freq = 'q', .return_vintages = T) %>%
				filter(., date >= as_date('2020-01-01')) %>%
				transmute(
					.,
					forecast = x$forecast,
					form = 'd1',
					freq = 'q',
					varname = x$varname,
					vdate = vintage_date,
					date,
					value
					)
		}) %>%
		bind_rows(.)

	fred_data <<- fred_data
})

# ## Get NYF Forecasts ---------------------------------------------------
# local({
#
# 	nyf_url = 'https://www.newyorkfed.org/medialibrary/media/research/policy/nowcast/new-york-fed-staff-nowcast_data_2002-present.xlsx?la=en'
# 	download.file(nyf_url, file.path(tempdir(), 'nyfnow.xlsx'), mode = 'wb', quiet = TRUE)
#
# 	nyf_data =
# 		readxl::read_excel(file.path(tempdir(), 'nyfnow.xlsx'), sheet = 2, skip = 13) %>%
# 		rename(., vdate = 1) %>%
# 		mutate(., vdate = as_date(vdate)) %>%
# 		pivot_longer(., -vdate, names_to = 'date', values_to = 'value', values_drop_na = T) %>%
# 		mutate(., date = from_pretty_date(date, 'q')) %>%
# 		filter(., date >= as_date('2020-01-01')) %>%
# 		transmute(
# 			.,
# 			forecast = 'nyfnow',
# 			form = 'd1',
# 			freq = 'q',
# 			varname = 'gdp',
# 			vdate,
# 			date,
# 			value
# 			)
#
# 	nyf_data <<- nyf_data
# })


## Join and Validate ---------------------------------------------------
local({

	raw_data = bind_rows(
		fred_data
		# nyf_data
	)

	raw_data %>%
		group_by(., forecast) %>%
		filter(., vdate == max(vdate)) %>%
		ungroup(.) %>%
		print(.)

	raw_data <<- raw_data
})

## Export SQL Server ------------------------------------------------------------------
local({

	# Store in SQL
	model_values =
		raw_data %>%
		transmute(., forecast, form, vdate, freq, varname, date, value)

	rows_added_v1 = store_forecast_values_v1(db, model_values, .verbose = T)
	rows_added_v2 = store_forecast_values_v2(db, model_values, .verbose = T)


	# Log
	log_data = list(
		rows_added = rows_added_v2,
		last_vdate = max(raw_data$vdate),
		stdout = paste0(tail(read_lines(file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))), 500), collapse = '\n')
	)
	log_finish_in_db(db, run_id, JOB_NAME, 'external-import', log_data)
})

## Finalize ------------------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

