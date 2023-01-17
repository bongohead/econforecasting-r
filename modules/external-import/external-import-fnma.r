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
library(econforecasting)
library(lubridate)
library(reticulate)
use_virtualenv(file.path(EF_DIR, '.virtualenvs', 'econforecasting'))

## Load Connection Info ----------------------------------------------------------
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
run_id = log_start_in_db(db, JOB_NAME, 'external-import')


# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	message('***** Downloading Fannie Mae Data')

	fnma_dir = file.path(tempdir(), 'fnma')
	fs::dir_create(fnma_dir)

	message('***** FNMA dir: ', fnma_dir)

	fnma_links =
		GET('https://www.fanniemae.com/research-and-insights/forecast/forecast-monthly-archive') %>%
		content(., type = 'parsed') %>%
		xml2::read_html(.) %>%
		rvest::html_nodes('div.fm-accordion ul li a') %>%
		keep(., ~ str_detect(rvest::html_text(.), 'News Release| Forecast')) %>%
		map_dfr(., ~ tibble(
			url = str_trim(rvest::html_attr(., 'href')),
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
		imap_dfr(., function(x, i) {

			message(str_glue('Importing macro {i}'))

			raw_import = camelot$read_pdf(
				x$econ_forecast_path,
				pages = '1',
				flavor = 'stream',
				# Below needed to prevent split wrapping columns correctly https://www.fanniemae.com/media/42376/display
				column_tol = -2
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
		imap_dfr(., function(x, i) {

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
						str_detect(varname, '5-Year') ~ 'mort05y' # Dropped 5 year forecast
					),
					date = from_pretty_date(date, 'q'),
					value = as.numeric(str_replace_all(value, c(',' = '')))
				) %>%
				na.omit(.) %>%
				transmute(., vdate = as_date(x$vdate), varname, date, value)
			# As of Oct. 2022, 5 year mortgage forecast is dropped so only requires 2
			if (length(unique(clean_import$varname)) < 2) stop('Missing variable')

			return(clean_import)
		})

	fnma_data =
		bind_rows(fnma_clean_macro, fnma_clean_housing) %>%
		transmute(
			.,
			forecast = 'fnma',
			form = 'd1',
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
