#' Validated 3/26/23

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-cb'

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)
library(DBI, include.only = 'dbDisconnect')

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
if (!interactive()) send_output_to_log(file.path(Sys.getenv('LOG_DIR'), paste0(JOB_NAME, '.log')))
pg = connect_pg()
run_id = log_start_in_db(pg, JOB_NAME, 'external-import')

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	varnames_map = tribble(
		~ varname, ~ fullname,
		'gdp', 'Real GDP',
		'pce', 'Real consumer spending',
		'pdir', 'Residential investment',
		'pdin', 'Nonresidential investment',
		'govt', 'Total gov\'t spending',
		'ex', 'Exports',
		'im', 'Imports',
		'unemp', 'Unemployment rate (%)',
		'cpi', 'Core PCE Inflation (%Y/Y)', # should be pcepi!
		'ffr', 'Fed Funds (%, Midpoint, Period End)'
		) %>%
		mutate(., fullname = str_to_lower(fullname))

	html_content =
		request('https://www.conference-board.org/research/us-forecast/us-forecast') %>%
		req_perform %>%
		resp_body_html

	vintage_date =
		html_content %>%
		html_element(., '#productTypeText') %>%
		html_text %>%
		str_extract(., '[^|]+') %>%
		str_trim %>%
		fast_strptime(., format = '%B %d, %Y') %>%
		as_date

	iframe_src = html_content %>% html_element(., '#chConferences iframe') %>% html_attr(., 'src')
	table_content = paste0(iframe_src, 'dataset.csv') %>% read_tsv(., col_names = F)

	# First two rows are headers
	headers_fixed =
		table_content %>%
		.[1:2, ] %>%
		t %>%
		as.data.frame %>%
		as_tibble %>%
		fill(., V1, .direction = 'down') %>%
		mutate(., V2 = str_replace_all(V2, c('IV Q' = 'Q4', 'III Q' = 'Q3', 'II Q' = 'Q2', 'I Q' = 'Q1'))) %>%
		mutate(., col_index = 1:nrow(.), quarter = ifelse(is.na(V1) | is.na(V2), NA, paste0(V1, V2))) %>%
		mutate(., quarter = ifelse(col_index == 1, 'fullname', ifelse(is.na(quarter), paste0('drop_', 1:nrow(.)), quarter))) %>%
		.$quarter

	table_fixed =
		table_content %>%
		tail(., -2) %>%
		set_names(., headers_fixed) %>%
		select(., -contains(coll('*'))) %>%
		select(., -contains('drop'))

	final_data =
		table_fixed %>%
		pivot_longer(., cols = -fullname, names_to = 'date', values_to = 'value') %>%
		mutate(., date = from_pretty_date(date, 'q'), fullname = str_to_lower(fullname)) %>%
		inner_join(., varnames_map, by = 'fullname') %>%
		transmute(
			.,
			forecast = 'cb',
			form = 'd1',
			freq = 'q',
			varname,
			vdate = vintage_date,
			date,
			value
		)

	print(unique(final_data$varname))
	if (length(unique(final_data$varname)) != nrow(varnames_map)) stop('Missing variables!')

	raw_data <<- final_data
})


## Export Forecasts ------------------------------------------------------------------
local({

	# Store in SQL
	model_values = transmute(raw_data, forecast, form, vdate, freq, varname, date, value)

	rows_added_v1 = store_forecast_values_v1(pg, model_values, .verbose = T)
	rows_added_v2 = store_forecast_values_v2(pg, model_values, .verbose = T)

	# Log
	log_data = list(
		rows_added = rows_added_v2,
		last_vdate = max(raw_data$vdate),
		stdout = paste0(tail(read_lines(file.path(Sys.getenv('LOG_DIR'), paste0(JOB_NAME, '.log'))), 20), collapse = '\n')
	)

	log_finish_in_db(pg, run_id, JOB_NAME, 'external-import', log_data)
})

## Finalize ------------------------------------------------------------------
dbDisconnect(pg)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
