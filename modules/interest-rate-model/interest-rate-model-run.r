#'  Run this script on scheduler at noon Eastern daily
#'  - Last values to run are FFR/SOFR (1 day lag, 9am ET)
#'  - Treasury data releases on day of
#'  - Bloomberg data releases on day of
#'  - AFX data releases with 1 day lag (8am)
#' Historical vintage dates are assigned given these assumptions!
#'
#' TBD:
#' - Merge TFUT
#' - Add vintage testing for mortgage rates
#'
#' Audited 1/17/23

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'interest-rate-model-run'
BACKTEST_MONTHS = 36

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)
library(highcharter)
library(DBI)
library(data.table)
library(forecast, include.only = c('forecast', 'Arima'))
library(mgcv, include.only = c('gam', 'gam.fit', 's', 'te'))
library(furrr, include.only = c('future_map'))
library(future, include.only = c('plan', 'multisession'))

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
if (!interactive()) send_output_to_log(file.path(Sys.getenv('LOG_DIR'), paste0(JOB_NAME, '.log')))
pg = connect_pg()
run_id = log_start_in_db(pg, JOB_NAME, 'external-import')

hist = list()
submodels = list()

## Load Variable Defs ----------------------------------------------------------'
input_sources = get_query(pg, 'SELECT * FROM interest_rate_model_variables')

# Historical Data ----------------------------------------------------------

## FRED ----------------------------------------------------------
# Vintages release with 1-day lag relative to current vals.
local({

	# Get historical data with latest release date
	# Note that only a single vintage date is pulled (last available value for each data point).
	# This assumes for rate historical data there are no revisions
	message('***** Importing FRED Data')
	api_key = Sys.getenv('FRED_API_KEY')

	fred_data =
		input_sources %>%
		df_to_list	%>%
		keep(., \(x) x$hist_source == 'fred') %>%
		imap(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_obs(x$hist_source_key, api_key, .freq = x$hist_source_freq, .obs_start = '2010-01-01', .verbose = F) %>%
				transmute(., varname = x$varname, freq = x$hist_source_freq, date, value)
			}) %>%
		list_rbind %>%
		# For simplicity, assume all data with daily frequency is released the same day.
		# Only weekly/monthly data keeps the true vintage date.
		mutate(., vdate = case_when(
			freq == 'd' ~ date + days(1),
			str_detect(varname, 'mort') ~ date + days(7),
			TRUE ~ NA_Date_
		))

	hist$fred <<- fred_data
})

## TREAS ----------------------------------------------------------
local({

	treasury_data = c(
		'https://home.treasury.gov/system/files/276/yield-curve-rates-2011-2020.csv',
		paste0(
			'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/',
			2021:year(today('US/Eastern')),
			'/all?type=daily_treasury_yield_curve&field_tdr_date_value=2023&page&_format=csv'
			)
		) %>%
		map(., .progress = F, \(x) read_csv(x, col_types = 'c')) %>%
		list_rbind %>%
		pivot_longer(., cols = -c('Date'), names_to = 'varname', values_to = 'value') %>%
		separate(., col = 'varname', into = c('ttm_1', 'ttm_2'), sep = ' ') %>%
		mutate(
			.,
			varname = paste0('t', str_pad(ttm_1, 2, pad = '0'), ifelse(ttm_2 == 'Mo', 'm', 'y')),
			date = mdy(Date),
			) %>%
		transmute(
			.,
			vdate = date,
			freq = 'd',
			varname,
			date,
			value
		) %>%
		filter(., !is.na(value)) %>%
		filter(., varname %in% filter(input_sources, hist_source == 'treas')$varname)

	hist$treasury <<- treasury_data
})

## BLOOM  ----------------------------------------------------------
local({

	bloom_data =
		input_sources %>%
		df_to_list %>%
		keep(., \(x) x$hist_source == 'bloom') %>%
		map(., function(x) {

			req = request(str_glue(
				'https://www.bloomberg.com/markets2/api/history/{x$hist_source_key}%3AIND/PX_LAST?',
				'timeframe=5_YEAR&period=daily&volumePeriod=daily'
				))

			res =
				req %>%
				req_headers(
					'Host' = 'www.bloomberg.com',
					'Referer' = str_glue('https://www.bloomberg.com/quote/{x$source_key}:IND'),
					'Accept' = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
					`User-Agent` = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0',
					`Set-Cookie` = 'seen_uk=1',
					`Set-Cookie` = 'exp_pref=AMER'
					) %>%
				req_retry(max_tries = 5) %>%
				req_perform %>%
				resp_body_json %>%
				.[[1]] %>%
				.$price %>%
				map(., \(row) as_tibble(row)) %>%
				list_rbind %>%
				transmute(
					.,
					varname = x$varname,
					freq = 'd',
					date = as_date(dateTime),
					vdate = date,
					value
					) %>%
				na.omit

			# Add sleep due to bot detection
			Sys.sleep(runif(6, 3, 5))

			return(res)
		}) %>%
		list_rbind

	hist$bloom <<- bloom_data
})

## AFX  ----------------------------------------------------------
local({

	afx_data =
		request('https://us-central1-ameribor.cloudfunctions.net/api/rates') %>%
		req_retry(max_tries = 5) %>%
		req_perform %>%
		resp_body_json %>%
		keep(., ~ all(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y') %in% names(.))) %>%
		map(., function(x)
			as_tibble(x) %>%
				select(., all_of(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y'))) %>%
				mutate(., across(-date, \(y) as.numeric(y)))
			) %>%
		list_rbind %>%
		mutate(., date = ymd(date)) %>%
		pivot_longer(., -date, names_to = 'varname_scrape', values_to = 'value') %>%
		inner_join(
			.,
			select(filter(input_sources, hist_source == 'afx'), varname, hist_source_key),
			by = c('varname_scrape' = 'hist_source_key')
			) %>%
		distinct(.) %>%
		transmute(., vdate = date + days(1), varname, freq = 'd', date, value)

	hist$afx <<- afx_data
})


## BOE  ----------------------------------------------------------
local({

	boe_keys = tribble(
		~ varname, ~ url,
		'sonia',
		str_glue(
			'https://www.bankofengland.co.uk/boeapps/database/fromshowcolumns.asp?',
			'Travel=NIxAZxSUx&FromSeries=1&ToSeries=50&DAT=RNG',
			'&FD=1&FM=Jan&FY=2017',
			'&TD=31&TM=Dec&TY={year(today("GMT"))}',
			'&FNY=Y&CSVF=TT&html.x=66&html.y=26&SeriesCodes=IUDSOIA&UsingCodes=Y&Filter=N&title=IUDSOIA&VPD=Y'
			),
		'ukbankrate', 'https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp'
	)

	boe_data = lapply(df_to_list(boe_keys), function(x)
		request(x$url) %>%
			req_retry(max_tries = 5) %>%
			req_perform %>%
			resp_body_html %>%
			html_node(., '#stats-table') %>%
			html_table(.) %>%
			set_names(., c('date', 'value')) %>%
			mutate(., date = dmy(date)) %>%
			arrange(., date) %>%
			filter(., date >= as_date('2010-01-01')) %>%
			# Fill in missing dates - to the latter of yesterday or max available date in dataset
			left_join(
				tibble(date = seq(min(.$date), to = max(max(.$date), today('GMT') - days(1)), by = '1 day')),
				.,
				by = 'date'
				) %>%
			mutate(., value = zoo::na.locf(value)) %>%
			transmute(., vdate = date, varname = x$varname, freq = 'd', date, value)
		) %>%
		bind_rows(.)

	 hist$boe <<- boe_data
})


## Store in SQL ----------------------------------------------------------
local({

	message('**** Storing SQL Data')

	hist_values =
		bind_rows(hist) %>%
		bind_rows(
			.,
			filter(., freq %in% c('d', 'w')) %>%
				mutate(., date = floor_date(date, 'months'), freq = 'm') %>%
				group_by(., varname, freq, date) %>%
				summarize(., value = mean(value), .groups = 'drop')
			) %>%
		# vdate is the same as date
		mutate(., vdate = date, form = 'd1')

	initial_count = get_rowcount(pg, 'interest_rate_model_input_values')
	message('***** Initial Count: ', initial_count)

	sql_result =
		hist_values %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			dbExecute(pg, create_insert_query(
				x,
				'interest_rate_model_input_values',
				'ON CONFLICT (vdate, form, varname, freq, date) DO UPDATE SET value=EXCLUDED.value'
			))
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	message('***** Rows Added: ', get_rowcount(pg, 'interest_rate_model_input_values') - initial_count)

	hist_values <<- hist_values
})


# Sub-Models  ----------------------------------------------------------

## ICE: Futures  ----------------------------------------------------------
local({

	ice_codes = tribble(
		~ varname, ~ product_id, ~ hub_id, ~ expiry,
		'sonia', '20343', '23428', 1,
		'sonia', '20484', '23565', 3,
		# 'euribor', '15275', '17455', 3,
		'estr', '15274', '17454', 1,
		)

	# Last vintage date data not in charts history so get here
	# Note that this is unreliable!
	# Temporarily commented as of 3/3/22 due to instability issues, esp. with 1m SONIA
	# ice_raw_data_latest =
	# 	ice_codes %>%
	# 	purrr::transpose(.) %>%
	# 	map_dfr(., function(x) {
	# 		# Get top-level expiration IDs
	# 		httr::GET(str_glue(
	# 			'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
	# 			'getContractsAsJson=&productId={x$product_id}&hubId={x$hub_id}')
	# 			) %>%
	# 			httr::content(., as = 'parsed') %>%
	# 			# Filter out packs & bundles
	# 			keep(., ~ str_detect(.$marketStrip, 'Pack|Bundle', negate = T)) %>%
	# 			map_dfr(., function(z) tibble(
	# 				varname = x$varname,
	# 				expiry = x$expiry,
	# 				date = floor_date(parse_date(z$marketStrip, '%b%y'), 'months'),
	# 				market_id = as.character(z$marketId),
	# 				value = {
	# 					if (!is.null(z$lastPrice) & !is.null(z$lastTime)) z$lastPrice
	# 					else NULL
	# 				},
	# 				vdate = {
	# 					if (!is.null(z$lastPrice) & !is.null(z$lastTime)) as_date(parse_date_time(z$lastTime, '%m/%d/%Y %I:%M %p'))
	# 					else NULL
	# 				}
	# 			))
	# 	})

	ice_raw_data = list_rbind(map(df_to_list(ice_codes), function(x) {

		# Get top-level expiration IDs
		expiration_ids = request(str_glue(
			'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
			'getContractsAsJson=&productId={x$product_id}&hubId={x$hub_id}'
			)) %>%
			req_retry(max_tries = 5) %>%
			req_perform %>%
			resp_body_json %>%
			# Filter out packs & bundles
			keep(., ~ str_detect(.$marketStrip, 'Pack|Bundle', negate = T)) %>%
			map(., function(z) tibble(
				date = floor_date(parse_date(z$marketStrip, '%b%y'), 'months'),
				market_id = as.character(z$marketId)
			)) %>%
			list_rbind

		# Iterate through the expiration IDs
		list_rbind(map(df_to_list(expiration_ids), function(z)
			request(str_glue(
				'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
				'getHistoricalChartDataAsJson=&marketId={z$market_id}&historicalSpan=2'
				)) %>%
				req_retry(max_tries = 5) %>%
				req_perform %>%
				resp_body_json %>%
				.$bars %>%
				{tibble(vdate = map_chr(., ~ .[[1]]), value = map_dbl(., ~ .[[2]]))} %>%
				transmute(
					.,
					varname = x$varname,
					expiry = x$expiry,
					date = as_date(z$date),
					vdate = as_date(parse_date_time(vdate, orders = '%a %b %d %H:%M:%S %Y'), tz = 'ET'),
					value = 100 - value
					)
			))

		}))

	ice_data =
		ice_raw_data %>%
		# Now use only one-month futures if it's the only available data
		group_by(varname, vdate, date) %>%
		filter(., expiry == min(expiry)) %>%
		arrange(., date) %>%
		ungroup %>%
		# Get rid of forecasts for old observations
		filter(., date >= floor_date(vdate, 'month')) %>%
		transmute(., varname, freq = 'm', vdate, date, value)

	# ice_data %>%
	# 	filter(., vdate == max(vdate)) %>%
	# 	ggplot(.) +
	# 	geom_line(aes(x = date, y = value, color = varname))

	## Now calculate BOE Bank Rate
	spread_df =
		hist$boe %>%
		filter(freq == 'd') %>%
		pivot_wider(., id_cols = date, names_from = varname, values_from = value) %>%
		mutate(., spread = sonia - ukbankrate)

	spread_df %>%
		ggplot(.) +
		geom_line(aes(x = date, y = spread))

	# Monthly baserate forecasts (daily forecasts can be calculated later; see ENG section)
	ukbankrate =
		ice_data %>%
		filter(., varname == 'sonia') %>%
		expand_grid(., tibble(lag = 0:10)) %>%
		mutate(., lagged_vdate = vdate - days(lag)) %>%
		left_join(
			.,
			spread_df %>% transmute(., lagged_vdate = date, spread),
			by = 'lagged_vdate'
		) %>%
		group_by(., varname, freq, vdate, date, value)  %>%
		summarize(., trailing_lag_mean = mean(spread, na.rm = T), .groups = 'drop') %>%
		mutate(
			.,
			varname = 'ukbankrate',
			value = value - trailing_lag_mean
		) %>%
		select(., varname, freq, vdate, date, value)

	final_df = bind_rows(
		ice_data,
		ukbankrate
	)

	submodels$ice <<- final_df
})

## CME: Futures  ----------------------------------------------------------
local({

	# CME Group Data
	message('Starting CME data scrape...')
	cme_cookie =
		request('https://www.cmegroup.com/') %>%
		add_standard_headers %>%
		req_perform %>%
		get_cookies(., T) %>%
		.$ak_bmsc

	# Get CME Vintage Date
	last_trade_date =
		request('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/305/G?quoteCodes=null&_=') %>%
		add_standard_headers %>%
		list_merge(., headers = list('Host' = 'www.cmegroup.com', 'Cookie' = cme_cookie)) %>%
		req_perform %>%
		resp_body_json %>%
		.$tradeDate %>%
		parse_date_time(., 'd-b-Y') %>%
		as_date(.)

	# See https://www.federalreserve.gov/econres/feds/files/2019014pap.pdf for CME futures model
	# SOFR 3m tenor./
	cme_search_space = tribble(
		~ varname, ~ cme_id,
		'ffr', '305',
		'sofr', '8462',
		'sofr', '8463',
		'bsby', '10038',
		't02y', '10048',
		't05y', '10049',
		't10y', '10050',
		't30y', '10051'
	)

	cme_raw_data = list_rbind(map(df_to_list(cme_search_space), function(var) {

		quotes = request(paste0(
			'https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/',
			var$cme_id,
			'/G?quoteCodes=null&_='
			)) %>%
			add_standard_headers %>%
			list_merge(., headers = list('Host' = 'www.cmegroup.com', 'Cookie' = cme_cookie)) %>%
			req_perform %>%
			resp_body_json %>%
			.$quotes

		res = list_rbind(map(quotes, function(x) {
			if (x$priorSettle %in% c('0.000', '0.00', '-')) return() # Related bug in CME website
			tibble(
				vdate = last_trade_date,
				date = ymd(x$expirationDate),
				value = {
					if (str_detect(var$varname, '^t\\d\\dy$')) as.numeric(x$priorSettle)
					else 100 - as.numeric(x$priorSettle)
				},
				varname = var$varname,
				cme_id = var$cme_id
			)
		}))

		return(res)
	}))

	log_dump_in_db(pg, run_id, JOB_NAME, 'interest-rate-model', list(cme_raw_data = cme_raw_data))

	cme_raw_data %>%
		arrange(., date) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = cme_id))

	cme_data =
		cme_raw_data %>%
		# Now use only one-month SOFR futures if it's the only available data
		group_split(varname, date) %>%
		map(., function(x) {
			if (x$varname[[1]] == 'sofr' & nrow(x) == 2 & '8463' %in% x$cme_id) filter(x, cme_id == '8463')
			else if (nrow(x) == 1) x
			else stop('Error - Multiple Duplicates for non SOFR Values')
		}) %>%
		list_rbind %>%
		arrange(., date) %>%
		# Get rid of forecasts for old observations
		filter(., date >= floor_date(last_trade_date, 'month') & value != 100) %>%
		transmute(., varname, vdate = last_trade_date, date, value)

	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
	# 	df %>%
	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
	# 		View(.)

	## Barchart.com data
	message('Starting Barchart data scrape...')

	barchart_sources =
		# 3 year history + max of 5 year forecast
		tibble(date = seq(
			floor_date(today() - months(BACKTEST_MONTHS), 'month'),
			length.out = (3+5)*12,
			by = '1 month'
			)) %>%
		mutate(., year = year(date), month = month(date)) %>%
		left_join(
			.,
			tibble(month = 1:12, code = c('F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z')),
			by = 'month'
			) %>%
		expand_grid(
			.,
			tibble(
				varname = c('sofr', 'ffr', 't02y', 't05y', 't10y', 't30y'),
				ticker = c('SQ', 'ZQ', 'TU', 'TF', 'TO', 'TZ'),
				max_months_out = c(5, 5, 1, 1, 1, 1) * 12
				)
			) %>%
		mutate(
			.,
			code = paste0(ticker, code, str_sub(year, -2)),
			months_out = interval(floor_date(today(), 'month'), floor_date(date, 'month')) %/% months(1)
		) %>%
		filter(., months_out <= max_months_out) %>%
		transmute(., varname, code, date) %>%
		arrange(., date) %>%
		df_to_list

	# Check ahead of time whether the source is valid
	# This is a lot faster then checking it during the actual scraping process
	barchart_sources_valid = keep(barchart_sources, .progress = T, function(source) {
		request(paste0(
			'https://instruments-prod.aws.barchart.com/instruments/search/', source$code, '?region=us'
			)) %>%
			req_retry(max_tries = 5) %>%
			req_perform %>%
			resp_body_json %>%
			.$instruments %>%
			map_chr(., \(x) x$symbol) %>%
			some(., \(x) x == source$code)
		})

	cookies =
		request('https://www.barchart.com/futures/quotes/ZQV22/') %>%
		add_standard_headers %>%
		req_perform %>%
		get_cookies(., T)

	barchart_data = map(barchart_sources, function(x) {

		print(str_glue('Pulling data for {x$varname} - {as_date(x$date)}'))
		Sys.sleep(runif(1, .4, 1))

		http_response =
			request(paste0(
				'https://www.barchart.com/proxies/timeseries/queryeod.ashx?',
				'symbol=', x$code, '&data=daily&maxrecords=640&volume=contract&order=asc',
				'&dividends=false&backadjust=false&daystoexpiration=1&contractroll=expiration'
			)) %>%
			req_retry(max_tries = 10) %>%
			add_standard_headers %>%
			list_merge(., headers = list(
				'X-XSRF-TOKEN' = URLdecode(str_extract(cookies$`XSRF-TOKEN`, '(?<==).*')),
				'Referer' = 'https://www.barchart.com/futures/quotes/ZQZ20',
				'Cookie' = URLdecode(paste0(
					'bcFreeUserPageView=0; webinar113WebinarClosed=true; ',
					paste0(cookies, collapse = '; ')
					))
			)) %>%
			req_perform

		if (http_response$status_code != 200) {
			print('No response, skipping')
			return(NULL)
		}

		res = http_response %>%
			resp_body_string  %>%
			read_csv(
				.,
				# Verified columns are correct - compare CME official chart to barchart
				col_names = c('contract', 'vdate', 'open', 'high', 'low', 'close', 'volume', 'oi'),
				col_types = 'cDdddddd'
			) %>%
			select(., contract, vdate, close) %>%
			mutate(
				.,
				varname = x$varname,
				vdate = vdate - days(1),
				date = as_date(x$date),
				value = ifelse(str_detect(varname, '^t\\d\\dy$'), close, 100 - close)
				)

		message('***** Rows: ', nrow(res))
		return(res)
		}) %>%
		compact %>%
		list_rbind %>%
		transmute(., varname, vdate, date, value) %>%
		filter(., date >= floor_date(vdate, 'month')) # Get rid of forecasts for old observations

	## Bloom forecasts
	# These are necessary to fill in missing BSBY forecasts for first 3 months before
	# date of first CME future
	bloom_data =
		hist$bloom %>%
		filter(., date == max(date) & varname == 'bsby') %>%
		transmute(., varname = 'bsby', vdate = max(cme_data$vdate), date = floor_date(date, 'months'), value)

	## Combine datasets and add monthly interpolation
	message('Adding monthly interpolation ...')
	final_df =
		cme_data %>%
		# Anti join in barchart data where CME data is missing
		bind_rows(., anti_join(barchart_data, ., by = c('varname', 'vdate', 'date'))) %>%
		# Replace Bloom futures with data
		full_join(., rename(bloom_data, bloom = value), by = c('varname', 'vdate', 'date')) %>%
		mutate(., value = ifelse(is.na(value), bloom, value)) %>%
		select(., -bloom) %>%
		# If this months forecast missing for BSBY, add it in for interpolation purposes
		# Otherwise the dataset starts 3 months out
		group_split(., vdate, varname) %>%
		map(., function(x) {
			x %>%
				# Join on missing obs dates
				right_join(
					.,
					tibble(
						varname = unique(x$varname),
						vdate = unique(x$vdate),
						date = seq(from = min(x$date), to = max(x$date), by = '1 month')
						),
					by = c('varname', 'vdate', 'date')
					) %>%
				arrange(date) %>%
				transmute(
					.,
					varname = unique(varname),
					freq = 'm',
					vdate = unique(vdate),
					date,
					value = zoo::na.approx(value)
				)
			}) %>%
		list_rbind %>%
		# Now smooth all values more than 2 years out due to low liquidity
		group_split(., vdate, varname) %>%
		map(., function(x)
			x %>%
				mutate(., value_smoothed = zoo::rollmean(value, 6, fill = NA, align = 'right')) %>%
				mutate(., value = ifelse(!is.na(value_smoothed) & date >= vdate + years(2), value_smoothed, value))
				) %>%
		list_rbind %>%
		select(., -value_smoothed)


	# Print diagnostics
	final_df %>%
		filter(., vdate == max(vdate)) %>%
		pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value') %>%
		arrange(., date)

	series_data =
		final_df %>%
		group_split(., varname) %>%
		imap(., function(x, i) list(
			name = x$varname[[1]],
			data =
				filter(x, vdate == max(vdate)) %>%
				mutate(date = datetime_to_timestamp(date)) %>%
				arrange(., date) %>%
				df_to_list(.) %>%
				map(., ~ list(.$date, round(.$value, 2))),
			color = rainbow(10)[i]
			))

	series_chart =
		highchart(type = 'stock') %>%
		reduce(
			series_data,
			function(accum, x) {
				hc_add_series(accum, name = x$name, data = x$data)
				},
			.init = .
			) %>%
		hc_add_theme(hc_theme_bloom()) %>%
		hc_legend(., enabled = TRUE)

	print(series_chart)

	submodels$cme <<- final_df
})

## TDNS: Nelson-Siegel Treasury Yield Forecast ----------------------------------------------------------
local({

	#' This relies heavily on expectations theory weighted to federal funds rate;
	#' Consider taking a weighted average with Treasury futures directly
	#' (see CME micro futures)
	#' https://www.bis.org/publ/qtrpdf/r_qt1809h.htm
	message('***** Adding Treasury Forecasts')

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		input_sources %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$')) %>%
		select(., varname) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))

	fred_data =
		bind_rows(hist$fred, hist$treasury) %>%
		filter(., str_detect(varname, '^t\\d{2}[m|y]$') | varname == 'ffr') %>%
		select(., -vdate)

	# Get all vintage dates that need to be backtested
	backtest_vdates =
		submodels$cme %>%
		filter(., varname == 'ffr') %>%
		transmute(., ffr = value, vdate, date) %>%
		# Only backtest vdates with at least 36 months of FFR forecasts
		group_by(., vdate) %>%
		mutate(., vdate_forecasts = n()) %>%
		filter(., vdate_forecasts >= 36) %>%
		# Limit to backtest vdates
		filter(., vdate >= today() - months(BACKTEST_MONTHS)) %>%
		group_by(., vdate) %>%
		summarize(.) %>%
		.$vdate

	# Get available historical data at each vintage date, up to 36 months of history
	hist_df_unagg =
		tibble(vdate = backtest_vdates) %>%
		mutate(., floor_vdate = floor_date(vdate, 'month')) %>%
		left_join(
			.,
			fred_data %>% mutate(., floor_date = floor_date(date, 'month')),
			# !! Only pulls dates that are strictly less than the vdate (1 day EFFR delay in FRED)
			join_by(vdate > date)
			) %>%
		# Get floor month diff
		left_join(
			.,
			distinct(., floor_vdate, floor_date) %>%
				mutate(., months_diff = interval(floor_date, floor_vdate) %/% months(1)),
			by = c('floor_vdate', 'floor_date')
		) %>%
		filter(., months_diff <= 36) %>%
		mutate(., is_current_month = floor_vdate == floor_date)

	hist_df_0 =
		hist_df_unagg %>%
		group_split(., is_current_month) %>%
		lapply(., function(df)
			if (df$is_current_month[[1]] == TRUE) {
				# If current month, use last available date
				df %>%
					group_by(., vdate, varname) %>%
					filter(., date == max(date)) %>%
					ungroup(.) %>%
					select(., vdate, varname, date, value) %>%
					mutate(., date = floor_date(date, 'months'))
			} else {
				# Otherwise use monthly average for history
				df %>%
					mutate(., date = floor_date) %>%
					group_by(., vdate, varname, date) %>%
					summarize(., value = mean(value), .groups = 'drop')
			}
		) %>%
		bind_rows(.) %>%
		arrange(., vdate, varname, date)

	# Create training dataset on spread
	hist_df =
		filter(hist_df_0, varname %in% yield_curve_names_map$varname) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(.,
			transmute(filter(hist_df_0, varname == 'ffr'), vdate, date, ffr = value),
			by = c('vdate', 'date')
			) %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	# Filter to only last 3 months
	train_df = hist_df %>% filter(., date >= add_with_rollback(vdate, months(-3)))

	#' Calculate DNS fit
	#'
	#' @param df: A tibble continuing columns date, value, and ttm
	#' @param return_all: FALSE by default.
	#' If FALSE, will return only the MSE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @details
	#' Recasted into data.table as of 12/21/22, significant speed improvements (600ms optim -> 110ms)
	#'	test = train_df %>% group_split(., vdate) %>% .[[1]]
	#'  microbenchmark::microbenchmark(
	#'  	optimize(get_dns_fit, df = test, return_all = F, interval = c(-1, 1), maximum = F)
	#'  	times = 10,
	#'  	unit = 'ms'
	#'	)
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			# mutate(f1 = 1, f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm), f3 = f2 - exp(-1 * lambda * ttm)) %>%
			# group_split(., date) %>%
			# map_dfr(., function(x) {
			# 	reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
			# 	bind_cols(x, fitted = fitted(reg)) %>%
			# 		mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
			# 		mutate(., resid = value - fitted)
			# 	}) %>%
			as.data.table(.) %>%
			.[, c('f1', 'f2') := list(1, (1 - exp(-1 * lambda * ttm))/(lambda * ttm))] %>%
			.[, f3 := f2 - exp(-1*lambda*ttm)] %>%
			split(., by = 'date') %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				x %>%
					.[, fitted := fitted(reg)] %>%
					.[, c('b1', 'b2', 'b3') := list(coef(reg)[['f1']], coef(reg)[['f2']], coef(reg)[['f3']])] %>%
					.[, resid := value - fitted] %>%
					.[, lambda := lambda]
				}) %>%
			rbindlist(.) %>%
			{if (return_all == FALSE) mean(abs(.$resid)) else as_tibble(.)}
	}

	# Find MSE-minimizing lambda value by vintage date
	optim_lambdas =
		train_df %>%
		group_split(., vdate) %>%
		map(., .progress = T, function(x) {
			list(
				vdate = x$vdate[[1]],
				train_df = x,
				lambda = optimize(
					get_dns_fit,
					df = x,
					return_all = F,
					interval = c(-1, 1),
					maximum = F
					)$minimum
				)
			})

	# Get historical DNS fits by vintage date
	dns_fit_hist = map_dfr(optim_lambdas, \(x) get_dns_fit(df = x$train_df, x$lambda, return_all = T))

	# Get historical DNS hists by vintage date
	dns_coefs_hist =
		dns_fit_hist %>%
		group_by(., vdate, date) %>%
		summarize(., lambda = unique(lambda), tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3), .groups = 'drop')

	# Check fits for last 12 vintage date (last historical fit for each vintage)
	dns_fit_plots =
		dns_fit_hist %>%
		filter(., vdate %in% tail(unique(dns_coefs_hist$vdate, 12))) %>%
		group_by(., vdate) %>%
		filter(., date == max(date)) %>%
		ungroup(.) %>%
		arrange(., vdate, ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted)) +
		facet_wrap(vars(vdate))

	print(dns_fit_plots)


	# For each vintage date, gets:
	#  monthly returns (annualized & compounded) up to 10 years (minus FFR) using TDNS
	#  decomposition to enforce smoothness, reweighted with historical values (via spline)
	# For each vdate, use the last possible historical date (b1, b2, b3) for TDNS.
	fitted_curve =
		dns_coefs_hist %>%
		group_by(., vdate) %>%
		slice_max(., date) %>%
		ungroup(.) %>%
		transmute(., vdate, tdns1, tdns2, tdns3, lambda) %>%
		expand_grid(., ttm = 1:480) %>%
		mutate(
			.,
			annualized_yield_dns =
				tdns1 +
				tdns2 * (1-exp(-1 * lambda * ttm))/(lambda * ttm) +
				tdns3 *((1-exp(-1 * lambda * ttm))/(lambda * ttm) - exp(-1 * lambda * ttm)),
			) %>%
		# Join on last historical datapoints for each vintage (for spline fitting)
		left_join(
			.,
			dns_fit_hist %>%
				group_by(., vdate) %>%
				slice_max(., date) %>%
				ungroup(.) %>%
				transmute(., vdate, ttm, yield_hist = value),
			by = c('vdate', 'ttm')
		) %>%
		group_split(., vdate) %>%
		map(., .progress = T, function(x)
			x %>%
				mutate(
					.,
					spline_fit = zoo::na.spline(
						c(.$yield_hist[1:360], rep(tail(keep(.$yield_hist, \(x) !is.na(x)), 1), 120)),
						method = 'natural',
						),
					annualized_yield = .5 * annualized_yield_dns + .5 * spline_fit,
					# Get dns_coefs yield
					cum_return = (1 + annualized_yield/100)^(ttm/12)
					)
		) %>%
		bind_rows(.) %>%
		select(., -tdns1, -tdns2, -tdns3, -lambda)

	# Check how well the spline combined with TDNS fits the history
	full_fit_plots =
		fitted_curve %>%
		filter(., vdate %in% c(head(unique(.$vdate, 6)), tail(unique(.$vdate), 6))) %>%
		ggplot(.) +
		geom_line(aes(x = ttm, y = annualized_yield_dns), color = 'blue') +
		geom_line(aes(x = ttm, y = spline_fit), color = 'black') +
		geom_point(aes(x = ttm, y = yield_hist), color = 'black') +
		geom_line(aes(x = ttm, y = annualized_yield), color = 'red') +
		facet_wrap(vars(vdate))

	print(full_fit_plots)


	# Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
	# and for each, iterate over the original "ttms" 1, 2, 3,
	# ..., 120 and for each forecast the cumulative return for the yttm period ahead.
	expectations_forecasts =
		yield_curve_names_map$ttm %>%
		lapply(., function(yttm)
			fitted_curve %>%
				arrange(., vdate, ttm) %>%
				group_by(., vdate) %>%
				mutate(
					.,
					yttm_ahead_cum_return = dplyr::lead(cum_return, yttm, order_by = ttm)/cum_return,
					yttm_ahead_annualized_yield = (yttm_ahead_cum_return^(12/yttm) - 1) * 100
				) %>%
				ungroup(.) %>%
				filter(., ttm <= 120) %>%
				mutate(., yttm = yttm) %>%
				inner_join(., yield_curve_names_map, c('yttm' = 'ttm'))
		) %>%
		bind_rows(.) %>%
		mutate(., date = add_with_rollback(floor_date(vdate, 'months'), months(ttm - 1))) %>%
		select(., vdate, varname, date, value = yttm_ahead_annualized_yield) %>%
		inner_join(
			.,
			submodels$cme %>%
				filter(., varname == 'ffr') %>%
				transmute(., ffr_vdate = vdate, ffr = value, date),
			join_by(date, closest(vdate >= ffr_vdate)) # Use last available ffr_vdate (but in practice always ==)
			) %>%
		# Only include forecasts where the ffr vdate was within 7 days of tyield vdate
		filter(., interval(ffr_vdate, vdate) %/% days(1) <= 7) %>%
		transmute(., vdate, varname, date, value = value + ffr)

	# Plot point forecasts
	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname)) +
		labs(title = 'Current forecast')

	# Plot curve forecasts
	expectations_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		hchart(., 'line', hcaes(x = ttm, y = value, color = date, group = date))

	# Plot forecasts over time for current period
	expectations_forecasts %>%
		filter(., date == floor_date(today(), 'months')) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = value, color = varname))

	# Check spread history
	expectations_forecasts %>%
		pivot_wider(., id_cols = c(vdate, date), names_from = varname, values_from = value) %>%
		mutate(., spread = t10y - t02y) %>%
		mutate(., months_ahead = interval(vdate, date) %/% months(1)) %>%
		select(., vdate, date, spread, months_ahead) %>%
		group_by(., vdate, months_ahead) %>%
		summarize(., spread = mean(spread), .groups = 'drop') %>%
		arrange(., vdate, months_ahead) %>%
		filter(., months_ahead %in% c(0, 12, 36, 60)) %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = spread, color = as.factor(months_ahead))) +
		labs(title = 'Forecasted 10-2 Spread (Months Ahead)', x = 'vdate', y = 'spread')

	# Calculate TDNS1, TDNS2, TDNS3 forecasts
	# Forecast vintage date should be bound to historical data vintage
	# date since reliant purely on historical data
	# Calculated TDNS1: TYield_10y
	# Calculated TDNS2: -1 * (t10y - t03m)
	# Calculated TDNS3: .3 * (2*t02y - t03m - t10y)
	dns_coefs_forecast =
		expectations_forecasts %>%
		select(., vdate, varname, date, value) %>%
		pivot_wider(id_cols = c('vdate', 'date'), names_from = 'varname', values_from = 'value') %>%
		transmute(
			.,
			vdate,
			date,
			tdns1 = t10y,
			tdns2 = -1 * (t10y - t03m),
			tdns3 = .3 * (2 * t02y - t03m - t10y)
		)


	## Now add historical ratios in for LT term premia

	# For 10-year forward forecast, use long-term spread forecast
	# Exact interpolatoin at 10-3 10 years ahead
	# These forecasts only account for expectations theory without accounting for
	# https://www.bis.org/publ/qtrpdf/r_qt1809h.htm
	# Get historical term premium
	term_prems_raw = collect(tbl(pg, sql(
		"SELECT vdate, varname, date, d1 AS value
		FROM forecast_values_v2_all
		WHERE forecast = 'spf' AND varname IN ('t03m', 't10y')"
	)))

	# Get long-term spreads forecast
	forecast_spreads = lapply(c('tbill', 'tbond'), function(var) {

		request(paste0(
			'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
			'survey-of-professional-forecasters/data-files/files/median_', var, '_level.xlsx?la=en'
			)) %>%
			req_perform(., path = file.path(tempdir(), paste0(var, '.xlsx'))) %>%
			readxl::read_excel(file.path(tempdir(), paste0(var, '.xlsx')), na = '#N/A', sheet = 'Median_Level') %>%
			select(., c('YEAR', 'QUARTER', paste0(str_to_upper(var), 'D'))) %>%
			na.omit %>%
			transmute(
				.,
				reldate = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q'),
				varname = {if (var == 'tbill') 't03m_lt' else 't10y_lt'},
				value = .[[paste0(str_to_upper(var), 'D')]]
			)
		}) %>%
		list_rbind(.) %>%
		# Guess release dates
		left_join(., term_prems_raw %>% group_by(., vdate) %>% summarize(., reldate = min(date)), by = 'reldate') %>%
		pivot_wider(., id_cols = vdate, names_from = varname, values_from = value) %>%
		mutate(., spread = t10y_lt - t03m_lt) %>%
		arrange(., vdate) %>%
		mutate(., spread = zoo::rollmean(spread, 2, fill = NA, align = 'right')) %>%
		tail(., -1)


	# 1. At each historical vdate, get the trailing 36-month historical spread between all Treasuries with the 3m yield
	# 2. Get the relative ratio of these historical spreads relative to the 10-3 spread (%diff from 0)
	# 3. Get the forecasted SPF long-term spread for the latest forecast available at each historical vdate
	# 4. Compare the forecast SPF long-term spread to the hist_spread_ratio
	# - Ex. Hist 10-3 spread = .5; hist 30-3 spread = 2 => 4x multiplier (spread_ratio)
	# - Forecast 10-3 spread = 1; want to get forecast 30-3; so multiply historical spread_ratio * 1
	# Get relative ratio of historical diffs to 10-3 year forecast to generate spread forecasts (from 3mo) for all variables
	# Then reweight these back in time towards other
	historical_ratios =
		hist_df %>%
		{left_join(
			filter(., varname != 't03m') %>% transmute(., date, vdate, ttm, varname, value),
			filter(., varname == 't03m') %>% transmute(., date, vdate, t03m = value),
			by = c('vdate', 'date')
		)} %>%
		mutate(., spread = value - t03m) %>%
		group_by(., vdate, varname, ttm) %>%
		summarize(., mean_spread_above_3m = mean(spread), .groups = 'drop') %>%
		arrange(., ttm) %>%
		{left_join(
			transmute(., vdate, varname, ttm, mean_spread_above_3m),
			filter(., varname == 't10y') %>% transmute(., vdate, t10y_mean_spread_above_3m = mean_spread_above_3m),
			by = c('vdate')
		)} %>%
		mutate(., hist_spread_ratio_to_10_3 = case_when(
			t10y_mean_spread_above_3m <= 0 ~ max(0, mean_spread_above_3m),
			mean_spread_above_3m <= 0 ~ 0,
			TRUE ~ mean_spread_above_3m /t10y_mean_spread_above_3m
		)) %>%
		# Sanity check
		mutate(., hist_spread_ratio_to_10_3 = ifelse(hist_spread_ratio_to_10_3 > 3, 3, hist_spread_ratio_to_10_3)) %>%
		arrange(., vdate)

	if (!all(sort(unique(historical_ratios$vdate)) == sort(backtest_vdates))) {
		stop ('Error: lost vintage dates!')
	}

	# Why so many *discontinuous* drops
	forecast_lt_spreads =
		historical_ratios %>%
		select(., vdate, ttm, varname, hist_spread_ratio_to_10_3) %>%
		left_join(
			.,
			transmute(forecast_spreads, hist_vdate = vdate, forecast_spread_10_3 = spread),
			join_by(closest(vdate >= hist_vdate))
		) %>%
		mutate(., forecast_lt_spread = hist_spread_ratio_to_10_3 * forecast_spread_10_3) %>%
		select(., vdate, varname, forecast_spread_10_3, forecast_lt_spread)

	forecast_lt_spreads %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = forecast_lt_spread, color = varname)) +
		geom_point(aes(x = vdate, y = forecast_spread_10_3, color = varname))


	# LT spread forecasts by date
	adj_forecasts_raw =
		expectations_forecasts %>%
		filter(., varname != 't03m') %>%
		left_join(
			.,
			transmute(filter(expectations_forecasts, varname == 't03m'), vdate, date, t03m = value),
			by = c('vdate', 'date')
		) %>%
		mutate(
			.,
			months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1),
			lt_weight = ifelse(months_ahead >= 60, .5, months_ahead/60 * .5),
			spread = value - t03m
			) %>%
		left_join(., forecast_lt_spreads, by = c('varname', 'vdate')) %>%
		mutate(., adj_spread = (1 - lt_weight) * spread + lt_weight * forecast_lt_spread) %>%
		mutate(., adj_value = adj_spread + t03m)

	adj_forecasts_raw %>%
		pivot_longer(., cols = c(value, adj_value)) %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = name)) +
		facet_wrap(vars(varname))

	adj_forecasts =
		adj_forecasts_raw %>%
		transmute(., vdate, varname, date, value = adj_value) %>%
		bind_rows(., filter(expectations_forecasts, varname == 't03m')) %>%
		arrange(., vdate, varname, date)


	# https://en.wikipedia.org/wiki/Logistic_function
	get_logistic_x0 = function(desired_y_intercept, k = 1) {
		res = log(1/desired_y_intercept - 1)/k
		return(res)
	}
	logistic = function(x, x0, k = 1) {
		res = 1/(1 + exp(-1 * k * (x - x0)))
		return(res)
	}

	# Join forecasts with historical data to smooth out bump between historical data and TDNS curve
	# TBD: Instead of merging against last_hist, it should be adjusted by the difference between the last_hist
	# and the last forecast for that last_hist
	# Use logistic smoother to reduce weight over time
	hist_merged_df =
		adj_forecasts %>%
		mutate(., months_ahead = interval(floor_date(vdate, 'months'), date) %/% months(1)) %>%
		left_join(
			.,
			# Use hist proper mean of existing data
			hist_df_unagg %>%
				mutate(., date = floor_date) %>%
				group_by(., vdate, varname, date) %>%
				summarize(., value = mean(value), .groups = 'drop') %>%
				# Keep lastest hist obs for each vdate
				group_by(., vdate) %>%
				filter(., date == max(date)) %>%
				ungroup(.) %>%
				transmute(., vdate, varname, date, last_hist = value),
			by = c('vdate', 'varname', 'date')
		) %>%
		group_by(., vdate, varname) %>%
		mutate(
			.,
			last_hist = zoo::na.locf(last_hist, na.rm = F),
			last_hist_diff = ifelse(is.na(last_hist), NA, last_hist - value)
		) %>%
		ungroup(.) %>%

		# Map weights with sigmoid function and fill down forecast month 0 histoical value for full vdate x varname
		mutate(
			.,
			# Scale multiplier for origin month (.5 to 1).
			# In the next step, this is the sigmoid curve's y-axis crossing point.
			# Swap to k = 1 in both logistic functions to force smoothness (original 4, .5)
			forecast_origin_y = logistic((1 - day(vdate)/days_in_month(vdate)), get_logistic_x0(1/3, k = 2), k = 2),
			# Goal: for f(x; x0) = 1/(1 + e^-k(x  - x0)) i.e. the logistic function,
			# find x0 s.t. f(0; x0) = forecast_origin_y => x0 = log(1/.75 - 1)/k
			forecast_weight =
				# e.g., see y=(1/(1+e^-x)+.25)/1.25 - starts at .75
				logistic(months_ahead, get_logistic_x0(forecast_origin_y, k = 2), k = 2) #log(1/forecast_origin_y - 1)/.5, .5)
				# (1/(1 + 1 * exp(-1 * months_ahead)) + (forecast_origin_y - (log(1/forecast_origin_y - 1))))
			) %>%
		# A vdate x varname with have all NAs if no history for forecast month 0 was available
		# (e.g. first day of the month)
		mutate(., final_value = ifelse(
			is.na(last_hist),
			value,
			forecast_weight * value + (1 - forecast_weight) * (value + last_hist_diff)
			)) %>%
		arrange(., desc(vdate))

	hist_merged_df %>%
		filter(., date == floor_date(today('US/Eastern'), 'month'), varname == 't10y') %>%
		ggplot(.) +
		geom_line(aes(x = vdate, y = forecast_weight, color = format(vdate, '%Y%m'))) +
		labs(title = 'Forecast weights for this months forecast over time')

	hist_merged_df %>%
		filter(., vdate == max(vdate), varname == 't30y') %>%
		ggplot(.) +
		geom_line(aes(x = date, y = forecast_weight)) +
		labs(title = 'Forecast weights generated on this vdate for future forecasts')

	# Test plot 1
	hist_merged_df %>%
		filter(., vdate %in% head(unique(hist_merged_df$vdate), 10)) %>%
		group_by(., vdate) %>%
		filter(., date == min(date)) %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), color = 'green') +
		geom_point(aes(x = ttm, y = value), color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), color = 'black') +
		facet_wrap(vars(vdate)) +
		labs(title = 'Forecasts - last 10 vdates', subtitle = 'green = joined, blue = unjoined_forecast, black = hist')

	# Test plot 2
	hist_merged_df %>%
		filter(., vdate == max(vdate) & date <= today() + years(1)) %>%
		inner_join(., yield_curve_names_map, by = 'varname') %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = final_value), color = 'green') +
		geom_point(aes(x = ttm, y = value), color = 'blue') +
		geom_point(aes(x = ttm, y = last_hist), color = 'black') +
		facet_wrap(vars(date)) +
		labs(title = 'Forecasts - 1vdate', subtitle = 'green = joined, blue = unjoined_forecast, black = hist')

	## TBD: WEIGHT OF MONTH 0 SHOULD DEPEND ON HOW FAR IT IS INTO TE MONTH
	merged_forecasts =
		hist_merged_df %>%
		transmute(., vdate, varname, freq = 'm', date, value = final_value)

	merged_forecasts %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value)) +
		facet_wrap(vars(varname))

	bind_rows(
		expectations_forecasts %>% filter(., vdate == max(vdate)) %>% mutate(., type = 'expectations'),
		adj_forecasts %>% filter(., vdate == max(vdate)) %>% mutate(., type = 'adj'),
		merged_forecasts %>% filter(., vdate == max(vdate)) %>% mutate(., type = 'merged')
		) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = type)) +
		facet_wrap(vars(varname))

	tdns <<- list(
		dns_coefs_hist = dns_coefs_hist,
		dns_coefs_forecast = dns_coefs_forecast
		)

	submodels$tdns <<- merged_forecasts
})


## TFUT: Smoothed DNS + Futures ----------------------------------------------------------
#' Temporarily disabled - since currently all these are merged into a single INT model
# local({
#
# 	"
# 	Fit splines on both temporal dimension, ttm dimension, and their tensor product
# 	https://www.math3ma.com/blog/the-tensor-product-demystified
# 	"
#
# 	# Match each varname x forecast date from TDNS to the closest futures vdate (within 7 day range)
# 	futures_df =
# 		submodels$tdns %>%
# 		left_join(
# 			.,
# 			submodels$cme %>%
# 				filter(., str_detect(varname, '^t\\d\\d[y|m]$')) %>%
# 				transmute(., date, futures_vdate = vdate, varname, futures_value = value),
# 			join_by(date, closest(vdate >= futures_vdate), varname)
# 		) %>%
# 		# Replace future_values that are too far lagged (before TDNS vdate) with NAs
# 		mutate(., futures_value = ifelse(futures_vdate >= vdate - days(7), futures_value, NA)) %>%
# 		group_by(., vdate) %>%
# 		mutate(., count_hist = sum(ifelse(!is.na(futures_value), 1, 0))) %>%
# 		ungroup(.) %>%
# 		filter(., count_hist >= 1) %>%
# 		mutate(
# 			.,
# 			ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1),
# 			months_out = interval(floor_date(vdate, 'month'), floor_date(date, 'month')) %/% months(1)
# 		) %>%
# 		mutate(., mod_value = ifelse(!is.na(futures_value), futures_value, value))
#
# 	# Can use furrr if too slow
# 	plan(multisession, workers = cl, gc = TRUE)
#
# 	future_forecasts =
# 		futures_df %>%
# 		group_split(., vdate) %>%
# 		tail(., 1) %>%
# 		map(., .progress = T, function(df, i) {
#
# 			fit = gam(
# 				mod_value ~
# 					s(months_out, bs = 'tp', fx = F, k = -1) +
# 					s(ttm, bs = 'tp', fx = F, k = -1) +
# 					te(ttm, months_out, k = 10),
# 				data = df,
# 				method = 'ML'
# 				)
#
# 			final_df = mutate(df, gam_fit = fit$fitted.values, final_value = .5 * value + .5 * gam_fit)
#
# 			plot =
# 				final_df %>%
# 				filter(., varname == 't10y') %>%
# 				arrange(., ttm) %>%
# 				ggplot(.) +
# 				geom_line(aes(x = date, y = value), color = 'green') +
# 				geom_point(aes(x = date, y = futures_value), color = 'black', size = 5) +
# 				geom_line(aes(x = date, y = mod_value), color = 'red') +
# 				geom_line(aes(x = date, y = gam_fit), color = 'pink') +
# 				geom_line(aes(x = date, y = final_value), color = 'blue')
#
# 			list(
# 				final_df = final_df,
# 				plot = plot
# 				)
# 			})
#
# 	future:::ClusterRegistry("stop")
#
# 	forecasts_df =
# 		map_dfr(future_forecasts, \(x) x$final_df) %>%
# 		transmute(., vdate, varname, freq = 'm', date, value)
#
# 	submodels$tfut <<- forecasts_df
# })


## CBOE: Futures ---------------------------------------------------------------------
local({

	cboe_data =
		request('https://www.cboe.com/us/futures/market_statistics/settlement/') %>%
		req_perform %>%
		resp_body_html %>%
		html_elements(., 'ul.document-list > li > a') %>%
		map(., function(x)
			tibble(
				vdate = as_date(str_sub(html_attr(x, 'href'), -10)),
				url = paste0('https://cboe.com', html_attr(x, 'href'))
			)
		) %>%
		map(., function(x)
			read_csv(x$url, col_names = c('product', 'symbol', 'exp_date', 'price'), col_types = 'ccDn', skip = 1) %>%
				filter(., product %in% c('AMB1', 'AMT1')) %>%
				transmute(
					.,
					varname = 'ameribor',
					product,
					vdate = as_date(x$vdate),
					date = floor_date(exp_date - months(1), 'months'),
					value = 100 - price/100
				)
			) %>%
		list_rbind %>%
		pivot_wider(., id_cols = c(varname, vdate, date), names_from = 'product', values_from = 'value') %>%
		mutate(., value = ifelse(
			is.na(AMB1) | is.na(AMT1),
			coalesce(AMB1, 0) + coalesce(AMT1, 0),
			(AMB1 + AMT1)/2
			)) %>%
		mutate(., value = smooth.spline(value)$y)

	cboe_data %>%
		filter(., vdate == max(vdate)) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value))

	# Plot comparison against TDNS
	cboe_data %>%
		filter(., vdate == max(vdate)) %>%
		bind_rows(
			.,
			filter(submodels$cme, varname == 'ffr' & vdate == max(vdate)),
			filter(submodels$cme, varname == 'sofr' & vdate == max(vdate))
			) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname, group = varname))

	ameribor_forecasts =
		filter(cboe_data, vdate == max(vdate)) %>%
		right_join(
			.,
			transmute(filter(submodels$cme, varname == 'sofr' & vdate == max(vdate)), date, sofr = value),
			by = 'date'
			) %>%
		mutate(., spread = value - sofr) %>%
		mutate(
			.,
			varname = 'ameribor',
			vdate = head(vdate, 1),
			spread = {c(
				na.omit(.$spread),
				forecast::forecast(Arima(.$spread, order = c(1, 1, 0)), length(.$spread[is.na(.$spread)]))$mean
				)},
			value = round(ifelse(!is.na(value), value, sofr + spread), 4)
			) %>%
		transmute(., varname, freq = 'm', vdate, date, value)

	submodels$cboe <<- ameribor_forecasts
})

## MOR: Mortgage Model ----------------------------------------------------------
local({

	# Calculate historical mortgage curve spreads
	input_df =
		# Get historical monthly averages
		bind_rows(hist$fred, hist$treasury) %>%
		filter(., varname %in% c('t10y', 't20y', 't30y', 'mort15y', 'mort30y')) %>%
		mutate(., date = floor_date(date, 'months')) %>%
		group_by(., varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop') %>%
		# Pivot out and calculate spreads
		pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value') %>%
		mutate(
			.,
			t15y = (t10y + t20y)/2,
			spread15 = mort15y - t15y, spread30 = mort30y - t30y
			) %>%
		# Join on latest DNS coefs
		# inner_join(., filter(tdns$dns_coefs_hist, vdate == max(vdate)), by = 'date') %>%
		# select(date, spread15, spread30, tdns1, tdns2) %>%
		select(., date, spread15, spread30) %>%
		arrange(., date) %>%
		mutate(., spread15.l1 = lag(spread15, 1), spread30.l1 = lag(spread30, 1)) %>%
		na.omit(.)

	pred_df =
		tdns$dns_coefs_forecast %>%
		filter(., vdate == max(vdate)) %>%
		bind_rows(tail(filter(input_df, date < .$date[[1]]), 1), .)

	# Include average of historical month as "forecast" period
	coefs15 = matrix(lm(spread15 ~ spread15.l1, input_df)$coef, nrow = 1)
	coefs30 = matrix(lm(spread30 ~ spread30.l1, input_df)$coef, nrow = 1)

	spread_forecast = reduce(2:nrow(pred_df), function(accum, x) {
		accum[x, 'spread15'] =
			coefs15 %*% matrix(as.numeric(
				# c(1, accum[[x, 'tdns1']], ... if included tdns1 as a covariate
				c(1, accum[x - 1, 'spread15']),
				nrow = 1
				))
		accum[x, 'spread30'] =
			coefs15 %*% matrix(as.numeric(
				c(1, accum[x - 1, 'spread30']),
				nrow = 1
			))
		return(accum)
		},
		.init = pred_df
		) %>%
		.[2:nrow(.),] %>%
		select(., date, spread15, spread30)

	mor_data =
		inner_join(
			spread_forecast,
			submodels$tdns %>%
				filter(., varname %in% c('t10y', 't20y', 't30y')) %>%
				filter(., vdate == max(vdate)) %>%
				pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value'),
			by = 'date'
		) %>%
		mutate(., mort15y = (t10y + t30y)/2 + spread15, mort30y = t30y + spread30) %>%
		select(., date, mort15y, mort30y) %>%
		pivot_longer(., -date, names_to = 'varname', values_to = 'value') %>%
		transmute(
			.,
			varname,
			freq = 'm',
			vdate = today(),
			date,
			value
			)

	submodels$mor <<- mor_data
})


## ENG: BoE Bank Rate (DAILY) ----------------------------------------------------------
# local({
#
# 	# Use monthly data from ICE
#
# 	# Now extract calendar dates of meetings
# 	old_dates = as_date(c(
# 		'2021-02-04', '2021-03-18', '2021-05-06', '2021-06-24', '2021-09-23', '2021-11-04', '2021-12-16'
# 		))
#
# 	new_dates =
# 		GET('https://www.bankofengland.co.uk/monetary-policy/upcoming-mpc-dates') %>%
# 		content(.) %>%
# 		html_nodes(., 'div.page-content') %>%
# 		lapply(., function(x) {
#
# 			div_year = str_sub(html_text(html_node(x, 'h2')), 0, 4)
#
# 			month_dates =
# 				html_text(html_nodes(x, 'table tbody tr td:nth-child(1)')) %>%
# 				str_replace(., paste0(wday(now() + days(0:6), label = T, abbr = F), collapse = '|'), '') %>%
# 				str_squish(.)
#
# 			paste0(month_dates, ' ', div_year) %>%
# 				dmy(.)
# 			}) %>%
# 		unlist(.) %>%
# 		as_date(.)
#
# 	# Join except for anything in new_dates already in old_dates
# 	meeting_dates =
# 		tibble(dates = old_dates, year = year(old_dates), month = month(old_dates)) %>%
# 		bind_rows(
# 			.,
# 			anti_join(
# 				tibble(dates = new_dates, year = year(new_dates), month = month(new_dates)),
# 				.,
# 				by = c('year', 'month')
# 			)
# 		) %>%
# 		.$dates
#
#
#
# 	## Daily calculations
# 	this_vdate = max(monthly_df$vdate)
# 	this_monthly_df = monthly_df %>% filter(., vdate == this_vdate) %>% select(., -vdate, -freq)
#
# 	# Designate each period as a constant-rate time between meetings
# 	periods_df =
# 		meeting_dates %>%
# 		tibble(period_start = ., period_end = lead(period_start, 1)) %>%
# 		filter(., period_start >= min(this_monthly_df$date) & period_end <= max(this_monthly_df$date))
#
#
# 	# For each period, see which months fall fully within them; if multiple, take the average;
# 	# most periods will by none
# 	periods_df %>%
# 		purrr::transpose(.) %>%
# 		map_dfr(., function(x)
# 			this_monthly_df %>%
# 				filter(., date >= x$period_start & ceiling_date(date, 'months') - days(1) <= x$period_end) %>%
# 				summarize(., filled_value = mean(value, na.rm = T)) %>%
# 				bind_cols(x, .) %>%
# 				mutate(., filled_value = ifelse(is.nan(filled_value), NA, filled_value))
# 			) %>%
# 		mutate(., period_start = as_date(period_start), period_end = as_date(period_end))
#
# 	submodels$eng <<-
# })


# Finalize ----------------------------------------------------------

## Store in SQL ----------------------------------------------------------
local({

	# Store in SQL
	submodel_values =
		bind_rows(
			submodels$ice,
			submodels$cme %>% filter(., !str_detect(varname, 't\\d\\d[m|y]')),
			submodels$tdns,
			submodels$cboe,
			submodels$mor
		) %>%
		transmute(., forecast = 'int', form = 'd1', vdate, freq, varname, date, value)

	rows_added_v1 = store_forecast_values_v1(pg, submodel_values, .verbose = T)
	rows_added_v2 = store_forecast_values_v2(pg, submodel_values, .verbose = T)

	# Log
	log_data = list(
		rows_added = rows_added_v2,
		last_vdate = max(submodel_values$vdate),
		stdout = paste0(tail(read_lines(file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))), 100), collapse = '\n')
	)
	log_finish_in_db(pg, run_id, JOB_NAME, 'interest-rate-model', log_data)

	submodel_values <<- submodel_values
})


## Close Connections ----------------------------------------------------------
dbDisconnect(pg)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
