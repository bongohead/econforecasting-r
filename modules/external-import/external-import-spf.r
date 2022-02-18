# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-spf'
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

	# Scrape vintage dates
	vintage_dates =
		httr::GET(paste0('https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
										 'survey-of-professional-forecasters/spf-release-dates.txt?'
		)) %>%
		httr::content(., as = 'text', encoding = 'UTF-8') %>%
		str_sub(
			.,
			str_locate(., coll('1990 Q2'))[1], str_locate(., coll('*The 1990Q2'))[1] - 1
		) %>%
		read_csv(., col_names = 'X1', col_types = 'c') %>%
		.$X1 %>%
		map_dfr(., function(x)
			x %>%
				str_squish(.) %>%
				str_split(., ' ') %>%
				.[[1]] %>%
				{
					if (length(.) == 4) tibble(X1 = .[1], X2 = .[2], X3 = .[3], X4 = .[4])
					else if (length(.) == 3) tibble(X1 = NA, X2 = .[1], X3 = .[2], X4 = .[3])
					else stop ('Error parsing data')
				}
		) %>%
		tidyr::fill(., X1, .direction = 'down') %>%
		transmute(
			.,
			release_date = from_pretty_date(paste0(X1, X2), 'q'),
			vdate = lubridate::mdy(str_replace_all(str_extract(X3, "[^\\s]+"), '[*]', ''))
		) %>%
		# Don't include first date - weirdly has same vintage date as second date
		filter(., release_date >= as_date('2000-01-01'))

	print(arrange(vintage_dates, desc(release_date)))
	
	# method: sv is for 'single value, refers to level'
	spf_params = tribble(
		~ varname, ~ spfname, ~ transform, ~ single_value,
		't03m', 'TBILL', 'base', F,
		't10y', 'TBOND', 'base', F,
		'gdp', 'RGDP', 'apchg', F,
		'ngdp', 'NGDP', 'apchg', F,
		'pce', 'RCONSUM', 'apchg', F,
		'govts', 'RSLGOV', 'apchg', F,
		'govtf', 'RFEDGOV', 'apchg', F,
		'pdir', 'RRESINV', 'apchg', F,
		'pdin', 'RNRESIN', 'apchg', F,
		'cbi', 'RCBI', 'base', F,
		'cpi', 'CPI', 'base', F,
		'pcepi', 'PCE', 'base', F,
		'unemp', 'UNEMP', 'base', F,
		'houst', 'HOUSING', 'base', F,
		'aaa', 'BOND', 'base', F,
		'baa', 'BAABOND', 'base', F,
		# 'inf5y', 'CPI5YR', 'base', T,
		# 'inf10y', 'CPI10', 'base', T,
		# 'infpce5y', 'PCE5YR', 'base', T,
		# 'infpce10y', 'PCE10', 'base', T
		)
	
	httr::GET(
		paste0(
			'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
			'survey-of-professional-forecasters/historical-data/medianlevel.xlsx?la=en'
			),
		httr::write_disk(file.path(tempdir(), paste0('spf.xlsx')), overwrite = TRUE)
		)
	spf_data_1_import =
		spf_params %>%
		purrr::transpose(.) %>%
		map_dfr(., function(x) {
			message(x$varname)
			readxl::read_excel(file.path(tempdir(), paste0('spf.xlsx')), na = '#N/A', sheet = x$spfname) %>%
				select(
					.,
					c('YEAR', 'QUARTER', {
						if (x$single_value == T && x$transform == 'base') x$spfname
						else if (x$transform == 'apchg') paste0(x$spfname, 1:6)
						else if (x$transform == 'base') paste0(x$spfname, 2:6)
						else stop('Error')
					})
				) %>%
				mutate(., release_date = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
				select(., -YEAR, -QUARTER) %>%
				tidyr::pivot_longer(., -release_date, names_to = 'fcPeriods') %>%
				mutate(., fcPeriods = {if (x$single_value == F) as.numeric(str_sub(fcPeriods, -1)) - 2 else 0}) %>%
				mutate(., date = add_with_rollback(release_date, months(fcPeriods * 3))) %>%
				arrange(., date) %>%
				na.omit(.) %>%
				inner_join(., vintage_dates, by = 'release_date') %>%
				transmute(
					.,
					varname = x$varname,
					vdate,
					date,
					value
				)
		})
	
	spf_data_1 = 
		spf_data_1_import %>%
		group_split(., vdate) %>%
		# Add in calculated variables
		map_dfr(., function(x)
			x %>%
				pivot_wider(., id_cols = c('vdate', 'date'), names_from = 'varname', values_from = 'value' ) %>% 
				arrange(., date) %>%
				mutate(
					.,
					pdi = pdir + pdin,
					govt = govts + govtf
					) %>%
				pivot_longer(., -c('vdate', 'date'), names_to = 'varname', values_to = 'value')
			) %>%
		na.omit(.) %>%
		group_split(., varname, vdate) %>%
		map_dfr(., function(x) {
			transform = x$varname[[1]] %in% c('gdp', 'ngdp', 'pce', 'pdi', 'pdir', 'pdin', 'govts', 'govtf', 'govt')
			x %>%
				mutate(., value = {if (transform == TRUE) apchg(value, 4) else value})
			}) %>%
		na.omit(.) %>%
		transmute(
			.,
			forecast = 'spf',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
			)


	## Download recession data
	httr::GET(
		paste0(
			'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
			'survey-of-professional-forecasters/data-files/files/median_recess_level.xlsx?la=en'
		),
		httr::write_disk(file.path(tempdir(), 'spf-recession.xlsx'), overwrite = TRUE)
	)
	spf_data_2 =
		readxl::read_excel(file.path(tempdir(), 'spf-recession.xlsx'), na = '#N/A') %>%
		mutate(., release_date = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
		select(., -YEAR, -QUARTER) %>%
		tidyr::pivot_longer(., -release_date, names_to = 'fcPeriods') %>%
		mutate(., fcPeriods = as.numeric(str_sub(fcPeriods, -1)) - 1) %>%
		mutate(., date = add_with_rollback(release_date, months(fcPeriods * 3))) %>%
		na.omit(.) %>%
		inner_join(., vintage_dates, by = 'release_date') %>%
		transmute(
			.,
			forecast = 'spf',
			form = 'd1',
			freq = 'q',
			varname = 'recess',
			vdate,
			date,
			value
		)
	
	spf_data = bind_rows(spf_data_1, spf_data_2) 
	
	raw_data <<- spf_data
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
