#'  Run this script on scheduler after close of business each day
#'  We can assume this data is not subject to revisions

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
					varname = x$varname, freq = 'd', date = as_date(dateTime), vdate = date + days(1),
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

## FFR & SOFR   ----------------------------------------------------------
local({
	
	
	
	
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
	dns_coefs_now = as.list(select(filter(dns_data, date == max(date)), tdns1, tdns2, tdns3))
	
	# Check fit on current data
	dns_fit =
		dns_fit_hist %>%
		filter(., date == max(date)) %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted))
	
	print(dns_fit)

	# Print forecasts
	# Requires FFR Forecasts
	# DIEBOLD LI FUNCTION SHOULD BE ffr + f1 + f2 () + f3()
	# Calculated TDNS1: TYield_10y
	# Calculated TDNS2: -1 * (t10y - t03m)
	# Calculated TDNS3: .3 * (2*t02y - t03m - t10y)
	# Keep these treasury yield forecasts as the external forecasts ->
	# note that later these will be "regenerated" in the baseline calculation,
	# may be off a bit due to calculation from TDNS, compare to
	
	# Monthly forecast up to 10 years
	# Get cumulative return starting from cur_date
	fitted_curve =
		tibble(ttm = seq(1: 480)) %>%
		dplyr::mutate(., cur_date = floor_date(today(), 'months')) %>%
		dplyr::mutate(
			.,
			annualized_yield =
				dns_coefs$b1 +
				dns_coefs$b2 * (1-exp(-1 * optim_lambda * ttm))/(optim_lambda * ttm) +
				dns_coefs$b3 * ((1-exp(-1 * optim_lambda * ttm))/(optim_lambda * ttm) - exp(-1 * optim_lambda * ttm)),
			# Get dns_coefs yield
			cum_return = (1 + annualized_yield/100)^(ttm/12)
		)
	
})






