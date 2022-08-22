# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-cb'
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
library(RPostgres)
library(econforecasting)
library(lubridate)

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
		'ffr', 'Fed Funds (%, Mid-point, Period End)'
		) %>%
		mutate(., fullname = str_to_lower(fullname))
	
	httr::set_config(config(ssl_verifypeer = FALSE))
	
	html_content =
		GET('https://www.conference-board.org/research/us-forecast/us-forecast') %>%
		content(.)
	
	vintage_date =
		html_content %>%
		html_element(., '#productTypeText') %>%
		html_text(.) %>%
		str_extract(., '[^|]+') %>%
		str_trim(.) %>%
		readr::parse_date(., format = '%B %d, %Y')
	
	
	table_el =
		html_content %>%
		html_element(., 'table.data_grid_table')
	
	# Try to match up table header line 1 & 2
	# table_el %>% rvest::html_table(., header = FALSE, trim = TRUE) 
	# Table header line 1
	th_1_raw =
		table_el %>%
		html_elements(., 'thead > tr:first-child > *') %>%
		lapply(., function(x) {
			# Return nothing if there's a rowspan
			# Retrun 1 if no colspan
			# Else return colspan counts
			if (!is.na(html_attr(x, 'rowspan', NA))) NA
			else if (is.na(html_attr(x, 'colspan', NA))) html_text(x)
			else rep(html_text(x), times = as.numeric(html_attr(x, 'colspan')))
		}) %>%
		unlist(.) %>%
		str_trim(.) %>%
		.[2:length(.)]
	
	# Skip multirow (full-year-indicators) at the end
	th_multirow_cells = length(keep(th_1_raw, is.na))
	
	th_1 = keep(th_1_raw, ~ !is.na(.))
	
	th_2 =
		table_el %>%
		html_elements(., 'thead > tr:nth-child(2) > *') %>%
		html_text(.) %>%
		str_trim(.) %>%
		.[2:length(.)]
	
	
	# Tbody scrape
	tb_varnames =
		table_el %>%
		html_elements(., 'tbody > tr > *:first_child')  %>%
		html_text(.) %>%
		str_to_lower(.)
	
	# nth-child(n+2) - 2+ positions onward
	# nth-child(-n+16) - all children up to position 16
	tb_data =
		table_el %>%
		html_elements(., 'tbody > tr') %>%
		lapply(., function(x) 
			# Note: need to add 1 to account for initial column skip
			html_elements(x, paste0('*:nth-child(n+2):nth-child(-n+', length(th_1) + 1,')')) %>%
				html_text(.)
			)
	
	# Now collapse back together
	full_data_raw = imap_dfr(tb_data, function(x, i) 
		tibble(
			year_raw = th_1,
			quarter_raw = th_2
			) %>%
			mutate(., is_hist = str_detect(quarter_raw, coll('*'))) %>%
			bind_cols(
				.,
				fullname = tb_varnames[[i]],
				value = x)
		)
	
	print(full_data_raw, n = 100)
	
	final_data =
		full_data_raw %>%
		filter(., is_hist == FALSE) %>%
		mutate(., date = paste0(year_raw, 'Q', as.integer(as.roman(str_replace_all(quarter_raw, 'Q', ''))))) %>%
		mutate(., date = from_pretty_date(date, 'q')) %>%
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


## Export SQL Server ------------------------------------------------------------------
local({
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_values')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		raw_data %>%
		transmute(., forecast, form, vdate, freq, varname, date, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'forecast_values',
				'ON CONFLICT (forecast, vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_values')$count)
	message('***** Rows Added: ', final_count - initial_count)
	
	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'external-import', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count, last_vdate = max(raw_data$vdate)))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
	) %>%
		dbExecute(db, .)
})

## Finalize ------------------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
