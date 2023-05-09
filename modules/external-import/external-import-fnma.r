# Initialize ----------------------------------------------------------
# If F, adds data to the database that belongs to data vintages already existing in the database.
# Set to T only when there are model updates, new variable pulls, or old vintages are unreliable.
STORE_NEW_ONLY = T
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)
library(reticulate)

use_virtualenv(file.path(Sys.getenv('EF_DIR'), '.virtualenvs', 'econforecasting'))

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	message('***** Downloading Fannie Mae Data')

	fnma_dir = file.path(tempdir(), 'fnma')
	fs::dir_create(fnma_dir)

	message('***** FNMA dir: ', fnma_dir)

	fnma_links =
		request('https://www.fanniemae.com/research-and-insights/forecast/forecast-monthly-archive') %>%
		req_perform %>%
		resp_body_html %>%
		html_nodes('div.fm-accordion ul li a') %>%
		keep(., \(x) str_detect(html_text(x), 'News Release| Forecast')) %>%
		map_dfr(., \(x) tibble(
			url = str_trim(html_attr(x, 'href')),
			type = case_when(
				str_detect(html_text(x), 'News Release') ~ 'article',
				str_detect(html_text(x), 'Economic Forecast') ~ 'econ_forecast',
				str_detect(html_text(x), 'Housing Forecast') ~ 'housing_forecast',
				TRUE ~ NA_character_
			)
		)) %>%
		na.omit

	fnma_details =
		fnma_links %>%
		mutate(., group = (1:nrow(.) - 1) %/% 3) %>%
		pivot_wider(., id_cols = group, names_from = type, values_from = url) %>%
		na.omit %>%
		df_to_list %>%
		.[1:12] %>%
		imap(., function(x, i) {

			# if (i %% 10 == 0) message('Downloading ', i)

			vdate =
				request(paste0('https://www.fanniemae.com', x$article)) %>%
				req_perform %>%
				resp_body_html %>%
				html_node(., "div[data-block-plugin-id='field_block:node:news:field_date'] > div") %>%
				html_text %>%
				mdy

			request(paste0('https://www.fanniemae.com', x$econ_forecast)) %>%
				req_perform(., path = file.path(fnma_dir, paste0('econ_', vdate, '.pdf')))

			request(paste0('https://www.fanniemae.com', x$housing_forecast)) %>%
				req_perform(., path = file.path(fnma_dir, paste0('housing_', vdate, '.pdf')))

			tibble(
				vdate = vdate,
				econ_forecast_path = normalizePath(file.path(fnma_dir, paste0('econ_', vdate, '.pdf'))),
				housing_forecast_path = normalizePath(file.path(fnma_dir, paste0('housing_', vdate, '.pdf')))
			)
		}) %>%
		list_rbind(.)

	camelot = import('camelot')

	fnma_clean_macro = imap(df_to_list(fnma_details), function(x, i) {

		message(str_glue('Importing macro {i}'))

		raw_import = camelot$read_pdf(
			x$econ_forecast_path,
			pages = '1',
			flavor = 'stream',
			# Below needed to prevent split wrapping columns correctly https://www.fanniemae.com/media/42376/display
			column_tol = -2,
			edge_tol = 60
			#table_areas = list('100, 490, 700, 250')
			)[0]$df %>%
			as_tibble(.)

		col_names =
			df_to_list(raw_import[3, ])[[1]] %>%
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
		}) %>%
		list_rbind

	fnma_clean_housing = imap(df_to_list(fnma_details), function(x, i) {

		message(str_glue('Importing housing {i}'))

		raw_import = camelot$read_pdf(
			x$housing_forecast_path,
			pages = '1',
			flavor = 'stream',
			column_tol = -1,
			edge_tol = 60
			)[0]$df %>%
			as_tibble(.)

		col_names =
			df_to_list(raw_import[3, ])[[1]] %>%
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
		}) %>%
		list_rbind

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


## Export Forecast ------------------------------------------------------------------
local({

	# Store in SQL
	model_values = transmute(raw_data, forecast, form, vdate, freq, varname, date, value)

	rows_added = store_forecast_values_v2(pg, model_values, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(raw_data$vdate)

	disconnect_db(pg)
})
