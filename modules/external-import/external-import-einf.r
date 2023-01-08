# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-einf'
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
run_id = log_start_in_db(db, JOB_NAME, 'interest-rate-model')


# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	api_key = get_secret('FRED_API_KEY', file.path(EF_DIR, 'model-inputs', 'constants.yaml'))

	# Note: Cleveland Fed gives one-year forward rates
	# Combine these with historical data to calculate historical one-year trailing rates
	download.file(
		'https://www.clevelandfed.org/-/media/files/webcharts/inflationexpectations/inflation-expectations.xlsx',
		file.path(tempdir(), 'inf.xlsx'),
		mode = 'wb'
	)

	# Get vintage dates for each release

	## Latest vintage date - sometime the archives page doesn't have the latest data (Aug 2022)
	vintage_dates = paste0(
		'https://api.stlouisfed.org/fred/release/dates?release_id=10',
		'&api_key=', api_key,
		'&file_type=json'
		) %>%
		GET(.) %>%
		content(.) %>%
		.$release_dates %>%
		map(., ~ .$date) %>%
		unlist(., recursive = F) %>%
		as_date(.) %>%
		keep(., ~ . > as_date('2010-01-01'))

	# Deprecated 10/20/2022 due to source site update
	# vintage_dates_0 =
	# 	httr::GET(paste0(
	# 		'https://www.clevelandfed.org/en/our-research/',
	# 		'indicators-and-data/inflation-expectations.aspx'
	# 	)) %>%
	# 	httr::content(.) %>%
	# 	rvest::html_nodes('#RightAsideSPL > div > p') %>%
	# 	keep(., ~ str_detect(html_text(.), 'Last updated')) %>%
	# 	.[[1]] %>%
	# 	html_text(.) %>%
	# 	str_replace(., 'Last updated', '') %>%
	# 	mdy(.)
	#
	# vintage_dates_1 =
	# 	httr::GET(paste0(
	# 		'https://www.clevelandfed.org/en/our-research/',
	# 		'indicators-and-data/inflation-expectations/archives.aspx'
	# 	)) %>%
	# 	httr::content(.) %>%
	# 	rvest::html_nodes('div[itemprop="text"] ul li') %>%
	# 	keep(., ~ length(rvest::html_nodes(., 'a.download')) == 1) %>%
	# 	map_chr(., ~ str_extract(rvest::html_text(.), '(?<=released ).*')) %>%
	# 	mdy(.)
	#
	# vintage_dates_2 =
	# 	httr::GET(paste0(
	# 		'https://www.clevelandfed.org/en/our-research/',
	# 		'indicators-and-data/inflation-expectations/inflation-expectations-archives.aspx'
	# 	)) %>%
	# 	httr::content(.) %>%
	# 	rvest::html_nodes('ul.topic-list li time') %>%
	# 	map_chr(., ~ rvest::html_text(.)) %>%
	# 	mdy(.)

	vdate_map =
		tibble(vdate = vintage_dates) %>%
		mutate(., vdate0 = floor_date(vdate, 'months')) %>%
		group_by(., vdate0) %>% summarize(., vdate = max(vdate)) %>%
		arrange(., vdate0)

	# Now parse data and get inflation expectations
	einf_final =
		readxl::read_excel(file.path(tempdir(), 'inf.xlsx'), sheet = 'Expected Inflation') %>%
		rename(., vdate0 = 'Model Output Date') %>%
		pivot_longer(., -vdate0, names_to = 'ttm', values_to = 'yield') %>%
		mutate(
			.,
			vdate0 = as_date(vdate0), ttm = as.numeric(str_replace(str_sub(ttm, 1, 2), ' ', '')) * 12
		) %>%
		# Correct vdates
		inner_join(., vdate_map, by = 'vdate0') %>%
		select(., -vdate0) %>%
		filter(., vdate >= as_date('2015-01-01')) %>%
		group_split(., vdate) %>%
		map_dfr(., function(x)
			right_join(x, tibble(ttm = 1:360), by = 'ttm') %>%
				arrange(., ttm) %>%
				mutate(
					.,
					yield = zoo::na.spline(yield),
					vdate = unique(na.omit(vdate)),
					cur_date = floor_date(vdate, 'months'),
					cum_return = (1 + yield)^(ttm/12),
					yttm_ahead_cum_return = dplyr::lead(cum_return, 1)/cum_return,
					yttm_ahead_annualized_yield = (yttm_ahead_cum_return ^ 12 - 1) * 100,
					date = add_with_rollback(cur_date, months(ttm - 1))
				)
		) %>%
		transmute(
			.,
			varname = 'inf',
			freq = 'm',
			vdate,
			date,
			value = yttm_ahead_annualized_yield,
		) %>%
		na.omit(.)

	einf_chart =
		einf_final %>%
		filter(., month(vdate) %in% c(1, 6)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = as.factor(vdate)))

	print(einf_chart)


	# einf_final represents one-year trailing rates

	# For each vintage_date, get the historical data for the last 12 months available at that vintage
	hist_data =
		get_fred_data('CPIAUCSL', api_key, .return_vintages = T) %>%
		transmute(., date, vdate = vintage_date, value)

	einf_results =
		einf_final %>%
		group_split(., vdate) %>%
		imap_dfr(., function(x, i) {

			message(str_glue('Calculating expected inflation for vintage {as_date(x$vdate[[1]])}'))

			# Get last 12 historical values
			hist_values =
					hist_data %>%
					filter(., vdate <= x$vdate[[1]]) %>%
					group_by(., date) %>%
					filter(., vdate == max(vdate)) %>%
					ungroup(.) %>%
					arrange(., date) %>%
					tail(., 12) %>%
					select(., date, value)

			# Calculate monthly growth rate
			# (1+ yttm_ahead_annualized_yield(t))^12 = (1 + monthly_growth_rate(t)) *
			# (1 + yttm_ahead_annualized_yield(t+1))^(11/12)
			growth_forecast =
				x %>%
				mutate(., monthly_growth =  (1 + value/100)/((1 + lead(value/100, 1))^(11/12))) %>%
				select(., date, monthly_growth) %>%
				filter(., !date %in% hist_values$date)

			# Interpolate in missing growth_forecast values if necessary by using a reverse locf
			bind_rows(hist_values, growth_forecast) %>%
				left_join(
					tibble(date = seq(from = min(.$date), to = max(.$date), by = '1 month')),
					.,
					by = 'date'
					) %>%
				mutate(
					.,
					monthly_growth = ifelse(
						is.na(value) & is.na(monthly_growth),
						zoo::na.locf(monthly_growth, fromLast = T),
						monthly_growth
						),
					index = 1:nrow(.)
					) %>%
				# Get CPI values
				purrr::reduce(
					# From first non-NA row to last
					filter(., !is.na(monthly_growth))$index[[1]]:nrow(.),
					function(accum, row) {
						accum[row, 'value'] = accum[row, 'monthly_growth'] * accum[row - 1, 'value']
						return(accum)
					},
					.init = .
					) %>%
				# Now calculating trailing inflation
				mutate(., value = (value/lag(value, 12) - 1) * 100) %>%
				.[13:nrow(.),] %>%
				transmute(., date, vdate = x$vdate[[1]], value)
		}) %>%
		transmute(
			.,
			forecast = 'einf',
			form = 'd1',
			freq = 'm',
			varname = 'cpi',
			vdate,
			date,
			value
		)

	raw_data <<- einf_results
})


## Export to SQL ------------------------------------------------------------------
local({

	# Store in SQL
	model_values =
		raw_data %>%
		transmute(., forecast, form = 'd1', vdate, freq, varname, date, value)

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
