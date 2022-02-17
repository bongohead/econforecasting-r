# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-fnma'
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
library(reticulate)
use_virtualenv(file.path(EF_DIR, '.virtualenvs', 'econforecasting'))

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

	message('***** Downloading Fannie Mae Data')

	fnma_dir = file.path(tempdir(), 'fnma')
	fs::dir_create(fnma_dir)

	message('***** FNMA dir: ', fnma_dir)

	fnma_links =
		httr::GET('https://www.fanniemae.com/research-and-insights/forecast/forecast-monthly-archive') %>%
		httr::content(., type = 'parsed') %>%
		xml2::read_html(.) %>%
		rvest::html_nodes('div.fm-accordion ul li a') %>%
		purrr::keep(., ~ str_detect(rvest::html_text(.), 'News Release| Forecast')) %>%
		map_dfr(., ~ tibble(
			url = rvest::html_attr(., 'href'),
			type = case_when(
				str_detect(rvest::html_text(.), 'News Release') ~ 'article',
				str_detect(rvest::html_text(.), 'Economic Forecast') ~ 'econ_forecast',
				str_detect(rvest::html_text(.), 'Housing Forecast') ~ 'housing_forecast',
				TRUE ~ NA_character_
			)
		)) %>%
		na.omit(.)

	fnma_details =
		fnma_links %>%
		mutate(., group = (1:nrow(.) - 1) %/% 3) %>%
		pivot_wider(., id_cols = group, names_from = type, values_from = url) %>%
		na.omit(.) %>%
		purrr::transpose(.) %>%
		.[1:12] %>%
		purrr::imap_dfr(., function(x, i) {

			# if (i %% 10 == 0) message('Downloading ', i)

			vdate =
				httr::GET(paste0('https://www.fanniemae.com', x$article)) %>%
				httr::content(., type = 'parsed') %>%
				xml2::read_html(.) %>%
				rvest::html_node(., "div[data-block-plugin-id='field_block:node:news:field_date'] > div") %>%
				rvest::html_text(.) %>%
				mdy(.)

			httr::GET(
				paste0('https://www.fanniemae.com', x$econ_forecast),
				httr::write_disk(file.path(fnma_dir, paste0('econ_', vdate, '.pdf')), overwrite = TRUE)
			)

			httr::GET(
				paste0('https://www.fanniemae.com', x$housing_forecast),
				httr::write_disk(file.path(fnma_dir, paste0('housing_', vdate, '.pdf')), overwrite = TRUE)
			)

			tibble(
				vdate = vdate,
				econ_forecast_path = normalizePath(file.path(fnma_dir, paste0('econ_', vdate, '.pdf'))),
				housing_forecast_path = normalizePath(file.path(fnma_dir, paste0('housing_', vdate, '.pdf')))
			)
		})

	camelot = import('camelot')

	fnma_clean_macro =
		fnma_details %>%
		purrr::transpose(.) %>%
		purrr::imap_dfr(., function(x, i) {

			message(str_glue('Importing macro {i}'))

			raw_import = camelot$read_pdf(
				x$econ_forecast_path,
				pages = '1',
				flavor = 'stream',
				# Below needed to prevent split wrapping columns correctly https://www.fanniemae.com/media/42376/display
				column_tol = -1
				#table_areas = list('100, 490, 700, 250')
			)[0]$df %>%
				as_tibble(.)

			col_names =
				purrr::transpose(raw_import[3, ])[[1]] %>%
				str_replace_all(., coll(c('.' = 'Q'))) %>%
				paste0('20', .) %>%
				{ifelse(. == '20', 'varname', .)}

			clean_import =
				raw_import[4:nrow(raw_import), ] %>%
				set_names(., col_names) %>%
				pivot_longer(., -varname, names_to = 'date', values_to = 'value') %>%
				filter(., str_length(date) == 6 & str_detect(date, 'Q') & value != '') %>%
				mutate(
					.,
					varname = case_when(
						str_detect(varname, 'Gross Domestic Product') ~ 'gdp',
						str_detect(varname, 'Personal Consumption') ~ 'pce',
						str_detect(varname, 'Residential Fixed Investment') ~ 'pdir',
						str_detect(varname, 'Business Fixed Investment') ~ 'pdin',
						str_detect(varname, 'Government Consumption') ~ 'govt',
						str_detect(varname, 'Net Exports') ~ 'nx',
						str_detect(varname, 'Change in Business Inventories') ~ 'cbi',
						str_detect(varname, 'Consumer Price Index') & !str_detect(varname, 'Core') ~ 'cpi',
						str_detect(varname, 'PCE Chain Price Index') & !str_detect(varname, 'Core') ~ 'pcepi',
						str_detect(varname, 'Unemployment Rate') ~ 'unemp',
						str_detect(varname, 'Federal Funds Rate') ~ 'ffr',
						str_detect(varname, '1-Year Treasury') ~ 't01y',
						str_detect(varname, '10-Year Treasury') ~ 't10y'
					),
					date = from_pretty_date(date, 'q'),
					value = as.numeric(str_replace_all(value, c(',' = '')))
				) %>%
				na.omit(.) %>%
				transmute(., vdate = as_date(x$vdate), varname, date, value)

			if (length(unique(clean_import$varname)) < 12) stop('Missing variable')

			return(clean_import)
		})

	fnma_clean_housing =
		fnma_details %>%
		purrr::transpose(.) %>%
		purrr::imap_dfr(., function(x, i) {

			message(str_glue('Importing housing {i}'))

			raw_import = camelot$read_pdf(
				x$housing_forecast_path,
				pages = '1',
				flavor = 'stream',
				column_tol = -1
			)[0]$df %>%
				as_tibble(.)

			col_names =
				purrr::transpose(raw_import[3, ])[[1]] %>%
				str_replace_all(., coll(c('.' = 'Q'))) %>%
				paste0('20', .) %>%
				{ifelse(. == '20', 'varname', .)}

			clean_import =
				raw_import[4:nrow(raw_import), ] %>%
				set_names(., col_names) %>%
				pivot_longer(., -varname, names_to = 'date', values_to = 'value') %>%
				filter(., str_length(date) == 6 & str_detect(date, 'Q') & value != '') %>%
				mutate(
					.,
					varname = case_when(
						str_detect(varname, 'Total Housing Starts') ~ 'houst',
						# str_detect(varname, 'Total Home Sales') ~ 'hsold',
						str_detect(varname, '30-Year') ~ 'mort30y',
						str_detect(varname, '5-Year') ~ 'mort05y'
					),
					date = from_pretty_date(date, 'q'),
					value = as.numeric(str_replace_all(value, c(',' = '')))
				) %>%
				na.omit(.) %>%
				transmute(., vdate = as_date(x$vdate), varname, date, value)

			if (length(unique(clean_import$varname)) < 3) stop('Missing variable')

			return(clean_import)
		})

	fnma_data =
		bind_rows(fnma_clean_macro, fnma_clean_housing) %>%
		transmute(
			.,
			sourcename = 'fnma',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	message('***** Missing Variables:')
	message(
		c('gdp', 'pce', 'pdir', 'pdin', 'govt', 'nx', 'cbi', 'cpi', 'pcepi', 'unemp', 'ffr', 't01y', 't10y',
			'houst', 'mort30y', 'mort05y') %>%
			keep(., ~ !. %in% unique(fnma_data$varname)) %>%
			paste0(., collapse = ' | ')
		)

	raw_data <<- fnma_data
})


## Export SQL Server ------------------------------------------------------------------
local({

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_import_forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		raw_data %>%
		transmute(., sourcename, vdate, freq, varname, date, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'external_import_forecast_values',
				'ON CONFLICT (sourcename, vdate, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}


	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL: ', sum(unlist(sql_result)))

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_import_forecast_values')$count)
	message('***** Initial Count: ', final_count)
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
