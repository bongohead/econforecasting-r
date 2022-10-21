#'  Run this script on scheduler after close of business each day
#'  Assumes rate data is not subject to revisions

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'interest-rate-model-run'
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
library(highcharter)
library(forecast)

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
hist = list()
submodels = list()

## Load Variable Defs ----------------------------------------------------------'
input_sources = as_tibble(dbGetQuery(db, 'SELECT * FROM interest_rate_model_variables'))

# Historical Data ----------------------------------------------------------

## FRED ----------------------------------------------------------
local({

	message('***** Importing FRED Data')

	fred_data =
		input_sources %>%
		purrr::transpose(.)	%>%
		keep(., ~ .$hist_source == 'fred') %>%
		imap_dfr(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_data(x$hist_source_key, CONST$FRED_API_KEY, .freq = x$hist_source_freq, .return_vintages = F) %>%
				transmute(., varname = x$varname, freq = x$hist_source_freq, date, value) %>%
				filter(., date >= as_date('2010-01-01'))
			})

	hist$fred <<- fred_data
})

## BLOOM  ----------------------------------------------------------
local({

	bloom_data =
		input_sources %>%
		purrr::transpose(.) %>%
		keep(., ~ .$hist_source == 'bloom') %>%
		map_dfr(., function(x) {
			
			res = httr::GET(
				paste0(
					'https://www.bloomberg.com/markets2/api/history/', x$hist_source_key, '%3AIND/PX_LAST?',
					'timeframe=5_YEAR&period=daily&volumePeriod=daily'
					),
				add_headers(c(
					'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:105.0) Gecko/20100101 Firefox/105.0',
					'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
					'Accept-Encoding' = 'gzip, deflate, br',
					'Accept-Language' ='en-US,en;q=0.5',
					'Cache-Control'='no-cache',
					'Connection'='keep-alive',
					'DNT' = '1',
					'Host' = 'www.bloomberg.com',
					'Pragma'='no-cache',
					'Referer' = str_glue('https://www.bloomberg.com/quote/{x$source_key}:IND')
					))
				) %>%
				httr::content(., 'parsed') %>%
				.[[1]] %>%
				.$price %>%
				map_dfr(., ~ as_tibble(.)) %>%
				transmute(
					.,
					varname = x$varname, freq = 'd', date = as_date(dateTime),
					value
					) %>%
				na.omit(.)
			
			# Add sleep due to bot detection
			Sys.sleep(runif(1, 3, 5))
			
			return(res)
		})

	hist$bloom <<- bloom_data
})

## AFX  ----------------------------------------------------------
local({

	afx_data =
		httr::GET('https://us-central1-ameribor.cloudfunctions.net/api/rates') %>%
		httr::content(., 'parsed') %>%
		keep(., ~ all(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y') %in% names(.))) %>%
		map_dfr(., function(x)
			as_tibble(x) %>%
				select(., all_of(c('date', 'ON', '1M', '3M', '6M', '1Y', '2Y'))) %>%
				mutate(., across(-date, function(x) as.numeric(x)))
			) %>%
		mutate(., date = ymd(date)) %>%
		pivot_longer(., -date, names_to = 'varname_scrape', values_to = 'value') %>%
		inner_join(
			.,
			select(filter(input_sources, hist_source == 'afx'), varname, hist_source_key),
			by = c('varname_scrape' = 'hist_source_key')
			) %>%
		distinct(.) %>%
		transmute(., varname, freq = 'd', date, value)

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

	boe_data = lapply(purrr::transpose(boe_keys), function(x)
		httr::GET(x$url) %>%
			httr::content(., 'parsed', encoding = 'UTF-8') %>%
			html_node(., '#stats-table') %>%
			html_table(.) %>%
			set_names(., c('date', 'value')) %>%
			mutate(., date = dmy(date)) %>%
			arrange(., date) %>%
			filter(., date >= as_date('2010-01-01')) %>%
			# Fill in missing dates - to the latter of yesterday or max available date in dataset
			left_join(tibble(date = seq(min(.$date), to = max(max(.$date), today('GMT') - days(1)), by = '1 day')), ., by = 'date') %>%
			mutate(., value = zoo::na.locf(value)) %>%
			transmute(., varname = x$varname, freq = 'd', date, value)
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

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM interest_rate_model_input_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		hist_values %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'interest_rate_model_input_values',
				'ON CONFLICT (vdate, form, varname, freq, date) DO UPDATE SET value=EXCLUDED.value'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM interest_rate_model_input_values')$count)
	message('***** Rows Added: ', final_count - initial_count)

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
	
	ice_raw_data =
		ice_codes %>%
		purrr::transpose(.) %>%
		map_dfr(., function(x) {
			
			# Get top-level expiration IDs
			httr::GET(str_glue(
				'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
				'getContractsAsJson=&productId={x$product_id}&hubId={x$hub_id}'
				)) %>%
				httr::content(., as = 'parsed') %>%
				# Filter out packs & bundles
				keep(., ~ str_detect(.$marketStrip, 'Pack|Bundle', negate = T)) %>%
				map_dfr(., function(z) tibble(
					date = floor_date(parse_date(z$marketStrip, '%b%y'), 'months'),
					market_id = as.character(z$marketId)
				)) %>%
				# Iterate through the expiration IDs
				purrr::transpose(.) %>%
				map_dfr(., function(z) 
					httr::GET(str_glue(
						'https://www.theice.com/marketdata/DelayedMarkets.shtml?',
						'getHistoricalChartDataAsJson=&marketId={z$market_id}&historicalSpan=2'
						)) %>%
						httr::content(., as = 'parsed') %>%
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
					)
			})
	
	ice_data =
		ice_raw_data %>%
		# Now use only one-month futures if it's the only available data
		group_by(varname, vdate, date) %>%
		filter(., expiry == min(expiry)) %>%
		arrange(., date) %>%
		ungroup(.) %>%
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
		httr::GET(
			'https://www.cmegroup.com/',
			add_headers(c(
				'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
				'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				'Accept-Encoding' = 'gzip, deflate, br',
				'Accept-Language' ='en-US,en;q=0.5',
				'Cache-Control'='no-cache',
				'Connection'='keep-alive',
				'DNT' = '1'
				))
			) %>%
		httr::cookies(.) %>%
		as_tibble(.) %>%
		filter(., name == 'ak_bmsc') %>%
		.$value

	# Get CME Vintage Date
	last_trade_date =
		httr::GET(
			paste0('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/305/G?quoteCodes=null&_='),
			add_headers(c(
				'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
				'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				'Accept-Encoding' = 'gzip, deflate, br',
				'Accept-Language' ='en-US,en;q=0.5',
				'Cache-Control'='no-cache',
				'Connection'='keep-alive',
				'Cookie'= cme_cookie,
				'DNT' = '1',
				'Host' = 'www.cmegroup.com'
			))
		) %>% content(., 'parsed') %>% .$tradeDate %>% lubridate::parse_date_time(., 'd-b-Y') %>% as_date(.)

	# See https://www.federalreserve.gov/econres/feds/files/2019014pap.pdf for CME futures model
	cme_raw_data =
		tribble(
			~ varname, ~ cme_id,
			'ffr', '305',
			'sofr', '8462',
			'sofr', '8463',
			'bsby', '10038'
		) %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(var)
			httr::GET(
				paste0('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/', var$cme_id, '/G?quoteCodes=null&_='),
				add_headers(c(
					'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
					'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
					'Accept-Encoding' = 'gzip, deflate, br',
					'Accept-Language' ='en-US,en;q=0.5',
					'Cache-Control'='no-cache',
					'Connection'='keep-alive',
					'Cookie'= cme_cookie,
					'DNT' = '1',
					'Host' = 'www.cmegroup.com'
					))
				) %>%
				httr::content(., as = 'parsed') %>%
				.$quotes %>%
				purrr::map_dfr(., function(x) {
					if (x$priorSettle %in% c('0.00', '-')) return() # Whack bug in CME website
					tibble(
						vdate = last_trade_date,
						date = lubridate::ymd(x$expirationDate),
						value = 100 - as.numeric(x$priorSettle),
						varname = var$varname,
						cme_id = var$cme_id
					)
				})
			)

	cme_data =
		cme_raw_data %>%
		# Now use only one-month SOFR futures if it's the only available data
		group_split(varname, date) %>%
		map_dfr(., function(x) {
			if (x$varname[[1]] == 'sofr' & nrow(x) == 2 & '8463' %in% x$cme_id) filter(x, cme_id == '8463')
			else if (nrow(x) == 1) x
			else stop('Error - Multiple Duplicates for non SOFR Values')
		}) %>%
		arrange(., date) %>%
		# Get rid of forecasts for old observations
		filter(., date >= lubridate::floor_date(last_trade_date, 'month') & value != 100) %>%
		transmute(., varname, vdate = last_trade_date, date, value)

	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
	# 	df %>%
	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
	# 		View(.)

	
	## Barchart.com data (currently just FFR)
	message('Starting Barchart data scrape...')
	
	
	barchart_sources =
		# 3 year history + 5 year forecast
		tibble(date = seq(floor_date(today() - years(3), 'month'), length.out = 8 * 12, by = '1 month')) %>%
		mutate(., year = year(date), month = month(date)) %>%
		left_join(
			.,
			tibble(month = 1:12, code = c('F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z')),
			by = 'month'
			) %>%
		expand_grid(., tibble(varname = c('sofr', 'ffr'), ticker = c('SQ', 'ZQ'))) %>%
		mutate(., code = paste0(ticker, code, str_sub(year, -2))) %>%
		transmute(., varname, code, date) %>%
		arrange(., date) %>%
		purrr::transpose(.)
	

	cookies =
		GET(
		'https://www.barchart.com/futures/quotes/ZQV22',
		) %>%
		cookies(.) %>%
		as_tibble(.)
	
	barchart_data = lapply(barchart_sources, function(x) {
		
		print(str_glue('Pulling data for {x$varname} - {as_date(x$date)}'))
		Sys.sleep(.5)
		
		http_response = RETRY(
			'GET',
			times = 10,
			paste0(
				'https://www.barchart.com/proxies/timeseries/queryeod.ashx?',
				'symbol=', x$code, '&data=daily&maxrecords=640&volume=contract&order=asc',
				'&dividends=false&backadjust=false&daystoexpiration=1&contractroll=expiration'
			),
			add_headers(c(
				'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0',
				'X-XSRF-TOKEN' = URLdecode(filter(cookies, name == 'XSRF-TOKEN')$value),
				'DNT' = '1',
				'Referer' = 'https://www.barchart.com/futures/quotes/ZQZ20',
				'Cookie' = URLdecode(paste0(
					'bcFreeUserPageView=0; webinar113WebinarClosed=true; ',
					paste0(paste0(cookies$name, '=', cookies$value), collapse = '; ')
					)),
				'Pragma' = 'no-cache',
				'Cache-Control' = 'no-cache'
				))
			)

		if (http_response$status_code != 200) {
			print('No response, skipping')
			return(NULL)
		}
		
		http_response %>%
			content(.) %>%
			read_csv(
				.,
				# Verified columns are correct - compare CME official chart to barchart
				col_names = c('contract', 'vdate', 'open', 'high', 'low', 'close', 'volume', 'oi'),
				col_types = 'cDdddddd'
			) %>%
			select(., contract, vdate, close) %>%
			mutate(., varname = x$varname, vdate = vdate - days(1), date = as_date(x$date), value = 100 - close) %>%
			return(.)
		}) %>%
		compact(.) %>%
		bind_rows(.) %>%
		transmute(., varname, vdate, date, value)	%>%	
		filter(., date >= lubridate::floor_date(vdate, 'month')) # Get rid of forecasts for old observations
	
	
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
		# Anti join in FFR
		bind_rows(
			.,
			anti_join(barchart_data, ., by = c('varname', 'vdate', 'date'))
		) %>%
		# Replace Bloom futures with data
		full_join(
			.,
			rename(bloom_data, bloom = value),
			by = c('varname', 'vdate', 'date')
		) %>%
		mutate(., value = ifelse(is.na(value), bloom, value)) %>%
		select(., -bloom) %>%
		# If this months forecast misisng for BSBY, add it in for interpolation purposes
		# Otherwise the dataset starts 3 motnhs out
		group_split(., vdate, varname) %>%
		map_dfr(., function(x) {
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
		# Now smooth all values more than 2 years out due to low liquidity
		group_split(., vdate, varname) %>%
		map_dfr(., function(x)
			x %>%
				mutate(., value_smoothed = zoo::rollmean(value, 6, fill = NA, align = 'right')) %>%
				mutate(., value = ifelse(!is.na(value_smoothed) & date >= vdate + years(2), value_smoothed, value))
				) %>%
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
				purrr::transpose(.) %>%
				map(., ~ list(.$date, round(.$value, 2))),
			color = rainbow(3)[i]
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

	bind_rows(
		tibble(logname = 'cme-raw-import', log_info = jsonlite::toJSON(cme_raw_data)),
		tibble(logname = 'cme-cleaned-import', log_info = jsonlite::toJSON(cme_data))
		) %>%
		mutate(., module = 'interest-rate-model', log_date = today(), log_group = 'data-store') %>%
		create_insert_query(
			.,
			'job_logs',
			'ON CONFLICT (logname, module, log_date, log_group)
				DO UPDATE SET log_info=EXCLUDED.log_info, log_dttm=CURRENT_TIMESTAMP'
			) %>%
		dbExecute(db, .)

	submodels$cme <<- final_df
})

## TDNS: Nelson-Siegel Treasury Yield Forecast ----------------------------------------------------------
local({

	message('***** Adding Calculated Variables')

	fred_data =
		hist$fred %>%
		filter(., str_detect(varname, 't\\d{2}[m|y]') | varname == 'ffr')
	
	today = today('US/Eastern')
	
	# Monthly aggregation & append EOM with current val
	fred_data_cat =
		fred_data %>%
		mutate(., is_current_month = year(today) == year(date) & month(today) == month(date)) %>%
		group_split(., is_current_month) %>%
		lapply(., function(df) 
			if (df$is_current_month[[1]] == TRUE) {
				# If current month, use last available date
				df %>%
 					group_by(., varname) %>%
					filter(., date == max(date)) %>%
					ungroup(.) %>%
					select(., varname, date, value) %>%
					mutate(., date = floor_date(date, 'months'))
			} else {
				# Otherwise use monthly average for history
				df %>%
					mutate(., date = floor_date(date, 'months')) %>%
					group_by(., varname, date) %>%
					summarize(., value = mean(value), .groups = 'drop')
			}
		) %>%
		bind_rows(.)

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		input_sources %>%
		filter(., str_detect(varname, 't\\d{2}[m|y]') & str_length(varname) == 4) %>%
		select(., varname) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))

	# Create training dataset from SPREAD from ffr - fitted on last 3 months
	hist_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		filter(., date >= add_with_rollback(today(), months(-90))) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(., transmute(filter(fred_data_cat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	train_df = hist_df %>% filter(., date >= add_with_rollback(today(), months(-3)))

	#' Calculate DNS fit
	#'
	#' @param df: A tibble continuing columns date, value, and ttm
	#' @param return_all: FALSE by default.
	#' If FALSE, will return only the MSE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			mutate(f1 = 1, f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm), f3 = f2 - exp(-1 * lambda * ttm)) %>%
			group_split(., date) %>%
			map_dfr(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				bind_cols(x, fitted = fitted(reg)) %>%
					mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
					mutate(., resid = value - fitted)
				}) %>%
			{if (return_all == FALSE) summarise(., mse = mean(abs(resid))) %>% .$mse else .}
	}

	# Find MSE-minimizing lambda value
	optim_lambda = optimize(
		get_dns_fit,
		df = train_df,
		return_all = FALSE,
		interval = c(-1, 1),
		maximum = FALSE
		)$minimum

	# Get historical DNS coefficients
	dns_fit_hist = get_dns_fit(df = hist_df, optim_lambda, return_all = TRUE)

	dns_coefs_hist =
		dns_fit_hist %>%
		group_by(., date) %>%
		summarize(., tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3))

	# Get last DNS coefs
	dns_coefs_now = as.list(select(filter(dns_coefs_hist, date == max(date)), tdns1, tdns2, tdns3))

	# Check fit on current data
	dns_fit =
		dns_fit_hist %>%
		filter(., date == max(date)) %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted))

	print(dns_fit)

	# Calculated TDNS1: TYield_10y
	# Calculated TDNS2: -1 * (t10y - t03m)
	# Calculated TDNS3: .3 * (2*t02y - t03m - t10y)

	# Monthly forecast up to 10 years (minus ffr)
	# Get cumulative return starting from cur_date
	fitted_curve =
		tibble(ttm = seq(1: 480)) %>%
		mutate(., cur_date = floor_date(today(), 'months')) %>%
		mutate(
			.,
			annualized_yield =
				dns_coefs_now$tdns1 +
				dns_coefs_now$tdns2 * (1-exp(-1 * optim_lambda * ttm))/(optim_lambda * ttm) +
				dns_coefs_now$tdns3 * ((1-exp(-1 * optim_lambda * ttm))/(optim_lambda * ttm) - exp(-1 * optim_lambda * ttm)),
			# Get dns_coefs yield
			cum_return = (1 + annualized_yield/100)^(ttm/12)
			)

	# Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
	# and for each, iterate over the original "ttms" 1, 2, 3,
	# ..., 120 and for each forecast the cumulative return for the yttm period ahead.
	treasury_forecasts =
		yield_curve_names_map$ttm %>%
		lapply(., function(yttm)
			fitted_curve %>%
				mutate(
					.,
					yttm_ahead_cum_return = dplyr::lead(cum_return, yttm)/cum_return,
					yttm_ahead_annualized_yield = (yttm_ahead_cum_return^(12/yttm) - 1) * 100
				) %>%
				filter(., ttm <= 120) %>%
				mutate(., yttm = yttm) %>%
				inner_join(., yield_curve_names_map, c('yttm' = 'ttm'))
		) %>%
		bind_rows(.) %>%
		mutate(
			.,
			date = add_with_rollback(cur_date, months(ttm - 1))
			) %>%
		select(., varname, date, value = yttm_ahead_annualized_yield) %>%
		inner_join(
			.,
			submodels$cme %>%
				filter(., varname == 'ffr') %>%
				filter(., vdate == max(vdate)) %>%
				transmute(., ffr = value, date),
			by = 'date'
			) %>%
		transmute(., varname, freq = 'm', date, vdate = today(), value = value + ffr)

	# Plot point forecasts
	treasury_forecasts %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname))

	# Plot curve forecasts
	treasury_forecasts %>%
		left_join(., yield_curve_names_map, by = 'varname') %>%
		hchart(., 'line', hcaes(x = ttm, y = value, color = date, group = date))

	# Calculate TDNS1, TDNS2, TDNS3 forecasts
	# Forecast vintage date should be bound to historical data vintage
	# date since reliant purely on historical data
	dns_coefs_forecast =
		treasury_forecasts %>%
		select(., varname, date, value) %>%
		pivot_wider(., names_from = 'varname') %>%
		transmute(
			.,
			date,
			tdns1 = t10y,
			tdns2 = -1 * (t10y - t03m),
			tdns3 = .3 * (2 * t02y - t03m - t10y)
		)

	tdns <<- list(
		dns_coefs_hist = dns_coefs_hist,
		optim_lambda = optim_lambda,
		dns_coefs_forecast = dns_coefs_forecast
		)
	submodels$tdns <<- treasury_forecasts
})

## CBOE: Futures ---------------------------------------------------------------------
local({

	cboe_data =
		httr::GET('https://www.cboe.com/us/futures/market_statistics/settlement/') %>%
		httr::content(.) %>%
		rvest::html_elements(., 'ul.document-list > li > a') %>%
		map_dfr(., function(x)
			tibble(
				vdate = as_date(str_sub(rvest::html_attr(x, 'href'), -10)),
				url = paste0('https://cboe.com', rvest::html_attr(x, 'href'))
			)
		) %>%
		purrr::transpose(.) %>%
		map_dfr(., function(x)
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
				forecast::forecast(forecast::Arima(.$spread, order = c(1, 1, 0)), length(.$spread[is.na(.$spread)]))$mean
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
		hist$fred %>%
		filter(., varname %in% c('t10y', 't20y', 't30y', 'mort15y', 'mort30y')) %>%
		mutate(., date = floor_date(date, 'months')) %>%
		group_by(., varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop') %>%
		pivot_wider(., id_cols = 'date', names_from = 'varname', values_from = 'value') %>%
		mutate(., t15y = (t10y + t20y)/2) %>%
		mutate(., spread15 = mort15y - t15y, spread30 = mort30y - t30y) %>%
		inner_join(., tdns$dns_coefs_hist, by = 'date') %>%
		select(date, spread15, spread30, tdns1, tdns2) %>%
		arrange(., date) %>%
		mutate(., spread15.l1 = lag(spread15, 1), spread30.l1 = lag(spread30, 1)) %>%
		na.omit(.)

	pred_df =
		tdns$dns_coefs_forecast %>%
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
		transmute(., varname, freq = 'm', vdate = today(), date, value)

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

	submodel_values =
		bind_rows(submodels) %>%
		transmute(., forecast = 'int', form = 'd1', vdate, freq, varname, date, value)

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		submodel_values %>%
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

	tribble(
		~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
		JOB_NAME, 'interest-rate-model', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
		) %>%
		create_insert_query(
			.,
			'job_logs',
			'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)

	submodel_values <<- submodel_values
})


## Close Connections ----------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
