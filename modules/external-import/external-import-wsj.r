# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-wsj'
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
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
run_id = log_start_in_db(db, JOB_NAME, 'external-import')


# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	message('**** WSJ Survey')
	message('***** Last Import Oct 22')

	wsj_params = tribble(
		~ submodel, ~ fullname,
		'wsj', 'WSJ Consensus',
		# 'wsj_wfc', 'Wells Fargo & Co.',
		# 'wsj_gsu', 'Georgia State University',
		# 'wsj_sp', 'S&P Global Ratings',
		# 'wsj_ucla', 'UCLA Anderson Forecast',
		# 'wsj_gs', 'Goldman, Sachs & Co.',
		# 'wsj_ms', 'Morgan Stanley'
	)

	xl_path = file.path(EF_DIR, 'modules', 'external-import', 'external-import-wsj.xlsx')

	wsj_data =
		readxl::excel_sheets(xl_path) %>%
		keep(., ~ str_extract(., '[^_]+') == 'wsj') %>%
		map_dfr(., function(sheetname) {

			vdate = str_extract(sheetname, '(?<=_).*')

			col_names = suppressMessages(readxl::read_excel(
				xl_path,
				sheet = sheetname,
				col_names = F,
				.name_repair = 'universal',
				n_max = 2
			)) %>%
				replace(., is.na(.), '') %>%
				{paste0(.[1,], '-', .[2,])}

			if (length(unique(col_names)) != length(col_names)) stop('WSJ Input Error!')

			readxl::read_excel(
				xl_path,
				sheet = sheetname,
				col_names = col_names,
				skip = 2
			) %>%
				pivot_longer(
					cols = -'-Forecast',
					names_to = 'varname_date',
					values_to = 'value'
				) %>%
				transmute(
					vdate = as_date(vdate),
					fullname = .$'-Forecast',
					varname = str_extract(varname_date, '[^-]+'),
					date = from_pretty_date(str_extract(varname_date, '(?<=-).*'), 'q'),
					value
				) %>%
				inner_join(., wsj_params, by = 'fullname') %>%
				transmute(., submodel, varname, freq = 'q', vdate, date, value)
		}) %>%
		na.omit(.) %>%
		group_split(., submodel, varname, freq, vdate) %>%
		purrr::imap_dfr(., function(z, i) {
			tibble(
				date = seq(from = min(z$date, na.rm = T), to = max(z$date, na.rm = T), by = '3 months')
			) %>%
				left_join(., z, by = 'date') %>%
				mutate(., value = zoo::na.approx(value)) %>%
				transmute(
					.,
					submodel = unique(z$submodel),
					varname = unique(z$varname),
					freq = unique(z$freq),
					vdate = unique(z$vdate),
					date,
					value
				)
		}) %>%
		transmute(
			.,
			forecast = 'wsj',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	raw_data <<- wsj_data
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
