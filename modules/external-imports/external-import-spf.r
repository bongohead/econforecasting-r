# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-forecasts-spf'
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
library(httr)
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
		~ varname, ~ spfname, ~ method, ~ single_value,
		't03m', 'TBILL', 'level', F,
		't10y', 'TBOND', 'level', F,
		'inf', 'CORECPI', 'level', F,
		'gdp', 'RGDP', 'growth', F,
		'ngdp', 'NGDP', 'growth', F,
		'pce', 'RCONSUM', 'growth', F,
		'govts', 'RSLGOV', 'growth', F,
		'govts', 'RFEDGOV', 'growth', F,
		'pdir', 'RRESINV', 'growth', F,
		'pdin', 'RNRESIN', 'growth', F,
		'inf', 'CPI', 'level', F,
		'infpce', 'PCE', 'level', F,
		'unemp', 'UNEMP', 'level', F,
		'cprofits', 'CPROF', 'growth', F,
		'houst', 'HOUSING', 'growth', F,
		'aaa', 'BOND', 'level', F,
		'baa', 'BAABOND', 'level', F,
		'inf5y', 'CPI5YR', 'level', T,
		'inf10y', 'CPI10', 'level', T,
		'infpce5y', 'PCE5YR', 'level', T,
		'infpce10y', 'PCE10', 'level', T
		)
	
	spf_data_1 = purrr::map_dfr(c('level', 'growth'), function(m) {
		
		httr::GET(
			paste0(
				'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
				'survey-of-professional-forecasters/historical-data/median', m, '.xlsx?la=en'
			),
			httr::write_disk(file.path(tempdir(), paste0('spf-', m, '.xlsx')), overwrite = TRUE)
		)
		
		spf_params %>%
			filter(., method == m) %>%
			purrr::transpose(.) %>%
			lapply(., function(x) {
				message(x$varname)
				readxl::read_excel(file.path(tempdir(), paste0('spf-', m, '.xlsx')), na = '#N/A', sheet = x$spfname) %>%
					select(
						.,
						c('YEAR', 'QUARTER', {
							if (x$single_value == T) x$spfname
							else if (m == 'level') paste0(x$spfname, 2:6)
							else if (m == 'growth') paste0('d', str_to_lower(x$spfname), 2:6)
							else stop('Error')
						})
					) %>%
					mutate(., release_date = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
					select(., -YEAR, -QUARTER) %>%
					tidyr::pivot_longer(., -release_date, names_to = 'fcPeriods') %>%
					mutate(., fcPeriods = {if (x$single_value == F) as.numeric(str_sub(fcPeriods, -1)) - 2 else 0}) %>%
					mutate(., date = add_with_rollback(release_date, months(fcPeriods * 3))) %>%
					na.omit(.) %>%
					inner_join(., vintage_dates, by = 'release_date') %>%
					transmute(
						.,
						varname = x$varname,
						freq = 'q',
						vdate,
						date,
						value
					)
			}) %>%
			bind_rows(.) %>%
			return(.)
		
	})
	
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
			varname = 'recess',
			freq = 'q',
			vdate,
			date,
			value
		)
	
	spf_data = bind_rows(spf_data_1, spf_data_2)
	
	
	raw_data <<- spf_data
})


## Export SQL Server ------------------------------------------------------------------
local({
	
	export_data =
		raw_data %>%
		transmute(
			.,
			sourcename = 'spf',
			freq = 'q',
			varname,
			vdate,
			date,
			value
			)
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_sources_data')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		hist$flat_last %>%
		.[form == 'd1' & freq == 'q' & varname %chin% filter(variable_params, dispgroup == 'GDP')$varname] %>%
		select(., varname, freq, form, date, value) %>%
		as_tibble(.) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'nowcast_model_hist_values',
				'ON CONFLICT (varname, freq, form, date) DO UPDATE SET value=EXCLUDED.value'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	
	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL: ', sum(unlist(sql_result)))
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM nowcast_model_hist_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)
	
})