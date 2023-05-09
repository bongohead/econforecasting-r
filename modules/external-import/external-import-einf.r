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
library(readxl, include.only = 'read_excel')

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	api_key = Sys.getenv('FRED_API_KEY')

	# Note: Cleveland Fed gives one-year forward rates
	# Combine these with historical data to calculate historical one-year trailing rates
	download.file(
		'https://www.clevelandfed.org/-/media/files/webcharts/inflationexpectations/inflation-expectations.xlsx',
		file.path(tempdir(), 'inf.xlsx'),
		mode = 'wb'
	)

	# Get vintage dates for each release

	## Latest vintage date - sometime the archives page doesn't have the latest data (Aug 2022)
	vintage_dates = request(paste0(
		'https://api.stlouisfed.org/fred/release/dates?release_id=10&api_key=', api_key, '&file_type=json'
		)) %>%
		req_perform %>%
		resp_body_json %>%
		.$release_dates %>%
		map(., \(x) x$date) %>%
		unlist(., recursive = F) %>%
		as_date %>%
		keep(., \(x) x > as_date('2010-01-01'))

	vdate_map =
		tibble(vdate = vintage_dates) %>%
		mutate(., vdate0 = floor_date(vdate, 'months')) %>%
		group_by(., vdate0) %>%
		summarize(., vdate = max(vdate), .groups = 'drop') %>%
		arrange(., vdate0)

	# Now parse data and get inflation expectations
	einf_final =
		read_excel(file.path(tempdir(), 'inf.xlsx'), sheet = 'Expected Inflation') %>%
		rename(., vdate0 = 'Model Output Date') %>%
		pivot_longer(., -vdate0, names_to = 'ttm', values_to = 'yield') %>%
		mutate(., vdate0 = as_date(vdate0), ttm = as.numeric(str_replace(str_sub(ttm, 1, 2), ' ', '')) * 12) %>%
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
		na.omit

	einf_chart =
		einf_final %>%
		filter(., month(vdate) %in% c(1, 6)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = as.factor(vdate)))

	print(einf_chart)

	# einf_final represents one-year trailing rates
	# For each vintage_date, get the historical data for the last 12 months available at that vintage
	hist_data = transmute(
		get_fred_data('CPIAUCSL', api_key, .return_vintages = T),
		date,
		vdate = vintage_date,
		value
		)

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
				reduce(
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


## Export Forecasts ------------------------------------------------------------------
local({

	# Store in SQL
	model_values = transmute(raw_data, forecast, form = 'd1', vdate, freq, varname, date, value)

	rows_added = store_forecast_values_v2(pg, model_values, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(raw_data$vdate)

	disconnect_db(pg)
})
