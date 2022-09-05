# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-cbo'
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

	cbo_vintages =
		map(0:2, function(page)
			paste0('https://www.cbo.gov/data/publications-with-data-files?page=', page, '') %>%
				httr::GET(.) %>%
				httr::content(.) %>%
				rvest::html_nodes('#block-cbo-cbo-system-main div.item-list > ul > li') %>%
				keep(
					.,
					~ str_detect(rvest::html_text(rvest::html_node(., 'span.views-field-title')), coll('Economic Outlook'))
				) %>%
				map_chr(., ~ rvest::html_text(rvest::html_node(., 'div.views-field-field-display-date')))
		) %>%
		unlist(.) %>%
		mdy(.) %>%
		tibble(release_date = .) %>%
		group_by(., month(release_date), year(release_date)) %>%
		summarize(., release_date = min(release_date), .groups = 'drop') %>%
		select(., release_date) %>%
		arrange(., release_date)

	url_params =
		httr::GET('https://www.cbo.gov/data/budget-economic-data') %>%
		httr::content(., type = 'parsed') %>%
		xml2::read_html(.) %>%
		rvest::html_nodes('div .view-content') %>%
		.[[9]] %>%
		rvest::html_nodes(., 'a') %>%
		map_dfr(., function(x) tibble(date = rvest::html_text(x), url = rvest::html_attr(x, 'href'))) %>%
		transmute(., date = mdy(paste0(str_sub(date, 1, 3), ' 1 ' , str_sub(date, -4))), url) %>%
		mutate(., date = as_date(date)) %>%
		inner_join(., cbo_vintages %>% mutate(., date = floor_date(release_date, 'months')), by = 'date') %>%
		transmute(., vdate = release_date, url = paste0('https://www.cbo.gov', url))

	cbo_params = tribble(
		~ varname, ~ cbo_category, ~ cbo_name, ~ cbo_units,
		'ngdp', 'Output', 'Gross Domestic Product (GDP)', 'Percentage change, annual rate',
		'gdp', 'Output', 'Real GDP', 'Percentage change, annual rate',
		'pce', 'Components of GDP (Real)', 'Personal Consumption Expenditures', 'Percentage change, annual rate',
		'pdi', 'Components of GDP (Real)', 'Gross Private Domestic Investment', 'Percentage change, annual rate',
		'pdin', 'Components of GDP (Real)', 'Nonresidential fixed investment', 'Percentage change, annual rate',
		'pdir', 'Components of GDP (Real)', 'Residential fixed investment', 'Percentage change, annual rate',
		'cbi', 'Components of GDP (Real)', 'Change in private inventories', 'Billions of chained (2012) dollars',
		'govt', 'Components of GDP (Real)', 'Government Consumption Expenditures and Gross Investment',
		'Percentage change, annual rate',
		'govtf', 'Components of GDP (Real)', 'Federal', 'Percentage change, annual rate',
		'govts', 'Components of GDP (Real)', 'State and local', 'Percentage change, annual rate',
		'nx', 'Components of GDP (Real)', 'Net Exports of Goods and Services', 'Billions of chained (2012) dollars',
		'ex', 'Components of GDP (Real)', 'Exports', 'Percentage change, annual rate',
		'im', 'Components of GDP (Real)', 'Imports', 'Percentage change, annual rate',
		'cpi', 'Prices', 'Consumer Price Index, All Urban Consumers (CPI-U)', 'Percentage change, annual rate',
		'pcepi', 'Prices', 'Price Index, Personal Consumption Expenditures (PCE)',
		'Percentage change, annual rate',
		'oil', 'Prices', 'Price of Crude Oil, West Texas Intermediate (WTI)', 'Dollars per barrel',
		'ffr', 'Interest Rates', 'Federal Funds Rate', 'Percent',
		't03m', 'Interest Rates', '3-Month Treasury Bill', 'Percent',
		't10y', 'Interest Rates', '10-Year Treasury Note', 'Percent',
		'unemp', 'Labor', 'Unemployment Rate, Civilian, 16 Years or Older', 'Percent',
		'lfpr', 'Labor', 'Labor Force Participation Rate, 16 Years or Older', 'Percent'
		)


	cbo_data =
		url_params %>%
		purrr::transpose(.) %>%
		imap_dfr(., function(x, i) {

			download.file(x$url, file.path(tempdir(), 'cbo.xlsx'), mode = 'wb', quiet = TRUE)

			# Not all spreadsheets start at the same row
			skip_rows =
				suppressMessages(readxl::read_excel(
					file.path(tempdir(), 'cbo.xlsx'),
					sheet = '1. Quarterly',
					skip = 0
				)) %>%
				mutate(., idx = 1:nrow(.)) %>%
				filter(., .[[1]] == 'Output') %>%
				{.$idx - 1}

			xl =
				suppressMessages(readxl::read_excel(
					file.path(tempdir(), 'cbo.xlsx'),
					sheet = '1. Quarterly',
					skip = skip_rows
				)) %>%
				rename(., cbo_category = 1, cbo_name = 2, cbo_name_2 = 3, cbo_units = 4) %>%
				mutate(., cbo_name = ifelse(is.na(cbo_name), cbo_name_2, cbo_name)) %>%
				select(., -cbo_name_2) %>%
				tidyr::fill(., cbo_category, .direction = 'down') %>%
				tidyr::fill(., cbo_name, .direction = 'down') %>%
				na.omit(.)

			xl %>%
				inner_join(., cbo_params, by = c('cbo_category', 'cbo_name', 'cbo_units')) %>%
				select(., -cbo_category, -cbo_name, -cbo_units) %>%
				pivot_longer(., -varname, names_to = 'date') %>%
				mutate(., date = from_pretty_date(date, 'q')) %>%
				filter(., date >= as_date(x$vdate)) %>%
				mutate(., vdate = as_date(x$vdate))

		}) %>%
		transmute(
			.,
			forecast = 'cbo',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	message('Missing variables...')
	message(cbo_params$varname %>% set_names(., .) %>% keep(., function(x) !x %in% cbo_data$varname))

	raw_data <<- cbo_data
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
