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

	message('**** WSJ Survey')
	message('***** Last Import Jan 22')
	
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
				'ON CONFLICT (forecast, form, vdate, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
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
