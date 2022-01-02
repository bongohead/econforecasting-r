#'  Run this script on scheduler after close of business each day
#'  Assumes rate data is not subject to revisions

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'PULL_YIELDS'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log')), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(paste0('Run ', Sys.Date()))
}

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)
library(highcharter)

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
models = list()

## Load Variable Defs ----------------------------------------------------------'
input_sources = tribble(
	~ varname, ~ input_type, ~ source, ~ source_key, ~ freq,
	'ffr', 'hist', 'FRED', 'EFFR', 'd',
	'sofr', 'hist', 'FRED',  'SOFR', 'd',
	'bsby', 'hist', 'BLOOM',  'BSBYON', 'd',
	'bsby01m', 'hist', 'BLOOM', 'BSBY1M', 'd',
	'bsby03m', 'hist', 'BLOOM', 'BSBY3M', 'd',
	'bsby06m', 'hist', 'BLOOM', 'BSBY6M', 'd',
	'bsby01y', 'hist', 'BLOOM', 'BSBY12M', 'd',
	'ameribor', 'hist', 'AFX', 'ON', 'd',
	'ameribor01m', 'hist', 'AFX', '1M', 'd',
	'ameribor03m', 'hist', 'AFX', '3M', 'd',
	'ameribor06m', 'hist', 'AFX', '6M', 'd',
	'ameribor01y', 'hist', 'AFX', '1Y', 'd',
	'ameribor02y', 'hist', 'AFX', '2Y', 'd',
	't01m', 'hist', 'FRED', 'DGS1MO', 'd',
	't03m', 'hist', 'FRED', 'DGS3MO', 'd',
	't06m', 'hist', 'FRED', 'DGS6MO', 'd',
	't01y', 'hist', 'FRED', 'DGS1', 'd',
	't02y', 'hist', 'FRED', 'DGS2', 'd',
	't05y', 'hist', 'FRED', 'DGS5', 'd',
	't07y', 'hist', 'FRED', 'DGS7', 'd',
	't10y', 'hist', 'FRED', 'DGS10', 'd',
	't20y', 'hist', 'FRED', 'DGS20', 'd',
	't30y', 'hist', 'FRED', 'DGS30', 'd'
	)


forecast_sources = tribble(
	~ forecastname, 
	'cme',
	'tdns',
	'ameribor'
)


# Historical Data ----------------------------------------------------------

## FRED ----------------------------------------------------------
local({

	message('***** Importing FRED Data')

	fred_data =
		input_sources %>%
		purrr::transpose(.)	%>%
		purrr::keep(., ~ .$source == 'FRED') %>%
		purrr::imap_dfr(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_data(x$source_key, CONST$FRED_API_KEY, .freq = x$freq, .return_vintages = T, .verbose = F) %>%
				transmute(., varname = x$varname, freq = x$freq, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date('2010-01-01') & vdate >= as_date('2010-01-01'))
			})

	hist$fred <<- fred_data
})

## BLOOM  ----------------------------------------------------------
local({
	
	bloom_data =
		input_sources %>%
		purrr::transpose(.) %>%
		keep(., ~ .$source == 'BLOOM') %>%
		map_dfr(., function(x) {
			
			httr::GET(
				paste0(
					'https://www.bloomberg.com/markets2/api/history/', x$source_key, '%3AIND/PX_LAST?',
					'timeframe=5_YEAR&period=daily&volumePeriod=daily'
					),
				add_headers(c(
					'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
					'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
					'Accept-Encoding' = 'gzip, deflate, br',
					'Accept-Language' ='en-US,en;q=0.5',
					'Cache-Control'='no-cache',
					'Connection'='keep-alive',
					'DNT' = '1',
					'Host' = 'www.bloomberg.com',
					'Referer' = str_glue('https://www.bloomberg.com/quote/{x$source_key}:IND')
					))
				) %>%
				httr::content(., 'parsed') %>%
				.[[1]] %>%
				.$price %>%
				map_dfr(., ~ as_tibble(.)) %>%
				transmute(
					.,
					varname = x$varname, freq = 'd', date = as_date(dateTime), vdate = date,
					value
					) %>%
				na.omit(.)
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
		mutate(., date = ymd(date), vdate = date + days(1)) %>%
		pivot_longer(., -c('date', 'vdate'), names_to = 'varname_scrape', values_to = 'value') %>%
		inner_join(
			.,
			select(filter(input_sources, source == 'AFX'), varname, source_key),
			by = c('varname_scrape' = 'source_key')
			) %>%
		transmute(., varname, freq = 'd', date, vdate, value)
	
	hist$afx <<- afx_data
})


# Models  ----------------------------------------------------------

## CME   ----------------------------------------------------------
local({
	
	# Quandl Data
	message('Starting Quandl data scrape')
	quandl_data =
		purrr::map_dfr(1:24, function(j)
			read_csv(
				str_glue('https://www.quandl.com/api/v3/datasets/CHRIS/CME_FF{j}.csv?api_key={CONST$QUANDL_API_KEY}'),
				col_types = 'Ddddddddd'
				) %>%
				transmute(., vdate = Date, settle = Settle, j = j) %>%
				filter(., vdate >= as_date('2010-01-01'))
			) %>%
		transmute(
			.,
			varname = 'ffr',
			vdate,
			date = # Consider the forecasted period the vdate + j
				from_pretty_date(paste0(year(vdate), 'M', month(vdate)), 'm') %>%
				add_with_rollback(., months(j - 1), roll_to_first = TRUE),
			value = 100 - settle
		)
	
	if (RESET_SQL) dbExecute(db, 'DROP TABLE IF EXISTS rates_model_quandl')
	if (!'rates_model_quandl' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(db,
			'CREATE TABLE rates_model_quandl (
				varname VARCHAR(255),
				vdate DATE,
				date DATE,
				value NUMERIC (20, 4),
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT rates_model_quandl_pk PRIMARY KEY (varname, vdate, date)
				)'
			)
		dbExecute(db, 'SELECT create_hypertable(relation => \'rates_model_quandl\', time_column_name => \'vdate\')')
	}
	dbExecute(db, create_insert_query(
		quandl_data,
		'rates_model_quandl',
		'ON CONFLICT (varname, vdate, date) DO UPDATE SET value=EXCLUDED.value'
		))
	
	

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
	
	if (RESET_SQL) dbExecute(db, 'DROP TABLE IF EXISTS rates_model_cme_raw')
	if (!'rates_model_cme_raw' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(db,
			'CREATE TABLE rates_model_cme_raw (
			varname VARCHAR(255),
			cme_id VARCHAR(255),
			vdate DATE,
			date DATE,
			value NUMERIC (20, 4),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT rates_model_cme_raw_pk PRIMARY KEY (varname, cme_id, vdate, date)
			)'
		)
		dbExecute(db,  'SELECT create_hypertable(relation => \'rates_model_cme_raw\', time_column_name => \'vdate\')')
	}
	dbExecute(db, create_insert_query(
		cme_raw_data,
		'rates_model_cme_raw',
		'ON CONFLICT (varname, cme_id, vdate, date) DO UPDATE SET value=EXCLUDED.value'
		))
	
	cme_data =
		cme_raw_data %>%
		# Now average out so that there's only one value for each (varname, date) combo
		group_by(varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop') %>%
		arrange(., date) %>%
		# Get rid of forecasts for old observations
		filter(., date >= lubridate::floor_date(last_trade_date, 'month') & value != 100) %>%
		transmute(., varname, vdate = last_trade_date, date, value)

	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
	# 	df %>%
	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
	# 		View(.)
	if (RESET_SQL) dbExecute(db, 'DROP TABLE IF EXISTS rates_model_cme')
	if (!'rates_model_cme' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(db,
			'CREATE TABLE rates_model_cme (
			varname VARCHAR(255),
			vdate DATE,
			date DATE,
			value NUMERIC (20, 4),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT rates_model_cme_pk PRIMARY KEY (varname, vdate, date)
			)'
		)
		dbExecute(db,  'SELECT create_hypertable(relation => \'rates_model_cme\', time_column_name => \'vdate\')')
	}
	dbExecute(db, create_insert_query(
		cme_data,
		'rates_model_cme',
		'ON CONFLICT (varname, vdate, date) DO UPDATE SET value=EXCLUDED.value'
	))
	
	
	## Bloom forecasts
	# These are necessary to fill in missing BSBY forecasts for first 3 months before 
	# date of first CME future
	bloom_data =
		hist$bloom %>%
		filter(., vdate == max(vdate)) %>%
		mutate(
			.,
			date =
				case_when(
					varname == 'bsby' ~ date,
					varname == 'bsby01m' ~ add_with_rollback(date, months(1)),
					varname == 'bsby03m' ~ add_with_rollback(date, months(3)),
					varname == 'bsby06m' ~ add_with_rollback(date, months(6)),
					varname == 'bsby01y' ~ add_with_rollback(date, months(12))
					) %>% floor_date(., 'months')
			) %>%
		transmute(., varname = 'bsby', vdate = max(cme_data$vdate), date, value)
	
	## Combine datasets and add monthly interpolation
	message('Adding monthly interpolation ...')
	final_df =
		full_join(
			rename(quandl_data, quandl = value),
			rename(cme_data, cme = value),
			by = c('varname', 'vdate', 'date')
			) %>%
		mutate(., value = ifelse(!is.na(quandl), quandl, cme)) %>%
		select(., -quandl, -cme) %>%
		# Replace Bloom futures with data 
		full_join(
			.,
			rename(bloom_data, bloom = value),
			by = c('varname', 'vdate', 'date')
		) %>%
		mutate(., value = ifelse(!is.na(bloom), bloom, value)) %>%
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
					vdate = unique(vdate),
					date,
					value = zoo::na.spline(value)
				)
			})
	
	
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
	
	models$cme <<- final_df
})

## TDNS ----------------------------------------------------------
local({

	message('***** Adding Calculated Variables')

	fred_data =
		hist$fred %>%
		filter(., freq == 'd' & (str_detect(varname, 't\\d{2}[m|y]') | varname == 'ffr'))

	# Monthly aggregation & append EOM with current val
	fred_data_cat =
		fred_data %>%
		mutate(., date = floor_date(date, 'months')) %>%
		group_by(., varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop')

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		input_sources %>%
		filter(., freq == 'd' & (str_detect(varname, 't\\d{2}[m|y]'))) %>%
		select(., varname) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))

	# Create training dataset from SPREAD from ffr - fitted on last 3 months
	hist_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		filter(., date >= add_with_rollback(today(), months(-120))) %>%
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
			models$ffr %>% filter(., vdate == today()) %>% transmute(., ffr = value, date),
			by = 'date'
			) %>%
		transmute(., varname, date, vdate = today(), value = value + ffr)
	
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
	tdns_forecasts =
		treasury_forecasts %>%
		select(., varname, date, value) %>%
		pivot_wider(., names_from = 'varname') %>%
		transmute(
			.,
			date,
			tdns1 = t10y,
			tdns2 = -1 * (t10y - t03m),
			tdns3 = .3 * (2 * t02y - t03m - t10y)
		) %>%
		pivot_longer(., -date, names_to = 'varname') %>%
		transmute(., varname, date, vdate = today(), value)
	
	models$tdns <<- treasury_forecasts
})

## Ameribor ---------------------------------------------------------------------
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
				filter(., product == 'AMB1') %>%
				transmute(
					.,
					varname = 'ameribor',
					vdate = as_date(x$vdate),
					date = floor_date(exp_date - months(1), 'months'),
					value = 100 - price/100
				)
			)
	
	# Plot comparison against TDNS & 
	cboe_data %>%
		filter(., vdate == max(vdate)) %>%
		bind_rows(
			.,
			filter(models$cme, varname == 'ffr' & vdate == max(vdate)),
			filter(models$cme, varname == 'sofr' & vdate == max(vdate))
			) %>%
		ggplot(.) +
		geom_line(aes(x = date, y = value, color = varname, group = varname))
	
	ameribor_forecasts =
		cboe_data %>%
		right_join(., transmute(models$sofr, vdate, date, sofr = value), by = c('vdate', 'date')) %>%
		mutate(., spread = value - sofr) %>%
		mutate(
			.,
			spread = {c(
				na.omit(.$spread),
				forecast::forecast(forecast::Arima(.$spread, order = c(1, 1, 0)), length(.$spread[is.na(.$spread)]))$mean
				)},
			value = round(ifelse(!is.na(value), value, sofr + spread), 4)
			) %>%
		transmute(., varname, vdate, date, value)
	
	models$ameribor <<- ameribor_forecasts
})




