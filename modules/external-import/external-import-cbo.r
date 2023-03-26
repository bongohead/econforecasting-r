#' Validated 3/26/23

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-cbo'

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

	cbo_vintages = map(0:2, function(page)
		request(paste0('https://www.cbo.gov/data/publications-with-data-files?page=', page, '')) %>%
			req_perform %>%
			resp_body_html %>%
			html_nodes('#block-cbo-cbo-system-main div.item-list > ul > li') %>%
			keep(., \(x) str_detect(html_text(html_node(x, 'span.views-field-title')), coll('Economic Outlook'))) %>%
			map_chr(., \(x) html_text(html_node(x, 'div.views-field-field-display-date')))
		) %>%
		unlist %>%
		mdy %>%
		tibble(release_date = .) %>%
		group_by(., month(release_date), year(release_date)) %>%
		summarize(., release_date = min(release_date), .groups = 'drop') %>%
		select(., release_date) %>%
		arrange(., release_date)

	url_params =
		request('https://www.cbo.gov/data/budget-economic-data') %>%
		req_perform %>%
		resp_body_html %>%
		html_nodes('div .view-content') %>%
		.[[9]] %>%
		html_nodes(., 'a') %>%
		map_dfr(., \(x) tibble(date = html_text(x), url = html_attr(x, 'href'))) %>%
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


	cbo_data = imap(df_to_list(url_params), function(x, i) {

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
			na.omit

		xl %>%
			inner_join(., cbo_params, by = c('cbo_category', 'cbo_name', 'cbo_units')) %>%
			select(., -cbo_category, -cbo_name, -cbo_units) %>%
			pivot_longer(., -varname, names_to = 'date') %>%
			mutate(., date = from_pretty_date(date, 'q')) %>%
			filter(., date >= as_date(x$vdate)) %>%
			mutate(., vdate = as_date(x$vdate))

		}) %>%
		list_rbind %>%
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
