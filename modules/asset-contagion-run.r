# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'asset-contagion-run'
EF_DIR = Sys.getenv('EF_DIR')
IMPORT_DATE_START = '2015-01-01'
RESET_DATA = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(data.table)
library(jsonlite)
library(DBI)
library(RPostgres)
library(econforecasting)
library(lubridate)
library(roll)

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

# Modeling ----------------------------------------------------------

## Get Data ----------------------------------------------------------
funds =
	tbl(db, sql('SELECT * FROM asset_contagion_funds')) %>%
	collect(.) %>%
	arrange(., id) %>%
	as.data.table(.)

input_data =
	funds %>%
	purrr::transpose(.) %>%
	lapply(., function(x) {
		url =
			paste0(
				'https://query1.finance.yahoo.com/v7/finance/download/', x$ticker,
				'?period1=', as.numeric(as.POSIXct(as_date(IMPORT_DATE_START))),
				'&period2=', as.numeric(as.POSIXct(Sys.Date() + days(1))),
				'&interval=1d',
				'&events=history&includeAdjustedClose=true'
				)
		data.table::fread(url, showProgress = FALSE) %>%
			.[, c('Date', 'Adj Close')]	%>%
			set_names(., c('date', 'value')) %>%
			.[, value := (value/shift(value, 1) - 1) * 100] %>%
			.[2:nrow(.), ] %>%
			.[, c('usage', 'ticker') := list(x$usage, x$ticker)] %>%
			return(.)
		}) %>%
	rbindlist(.)

## Correlation Values ----------------------------------------------------------
cor_values = lapply(unique(funds$usage), function(this_usage) {
	
	eligible_funds = funds[usage == this_usage]
	
	eligible_data = input_data[usage == this_usage][, usage := NULL]
	
	# Calculate all possible combinations of tickers; then merge data into it
	merged_data =
		cross_df(list(ticker_1 = 1:nrow(eligible_funds), ticker_2 = 1:nrow(eligible_funds))) %>%
		as.data.table(.) %>%
		.[ticker_2 > ticker_1] %>%
		.[, ticker_1 := map_chr(.$ticker_1, ~ eligible_funds[[., 'ticker']])] %>%
		.[, ticker_2 := map_chr(.$ticker_2, ~ eligible_funds[[., 'ticker']])] %>%
		# Rename on 
		merge(
			.,
			rename(eligible_data, value_1 = value),
			by.x = 'ticker_1', by.y = 'ticker', all = F, allow.cartesian = T
			) %>%
		merge(
			.,
			rename(eligible_data, value_2 = value),
			by.x = c('ticker_2', 'date'), by.y = c('ticker', 'date'), all = F, allow.cartesian = F
			)

	cors =
		merged_data %>%
		.[order(ticker_1, ticker_2, date)] %>%
		.[,
			c('cor_07', 'cor_30', 'cor_60', 'cor_90') := list(
				roll_cor(value_1, value_2, width = 7),
				roll_cor(value_1, value_2, width = 30),
				roll_cor(value_1, value_2, width = 60),
				roll_cor(value_1, value_2, width = 90)
				),
			by = c('ticker_1', 'ticker_2')
			] %>%
		melt(
			.,
			id.vars = c('date', 'ticker_1', 'ticker_2'),
			measure = patterns('cor_'),
			variable.name = 'window',
			value.name = 'value',
			variable.factor = FALSE,
			na.rm = TRUE
		) %>%
		.[, window := as.integer(str_sub(window, -2))] %>%
		.[, usage := this_usage]
	
	return(cors)
	}) %>%
	rbindlist(.)


## Index Values ----------------------------------------------------------
local({
	
	cor_index =
		cor_values %>%
		.[window == 30] %>%
		.[, list(value = mean(value), count = .N), by = c('date', 'usage')] %>%
		.[, value := round(value, 4)] %>%
		.[, list(value = mean(value)), by = 'date'] %>%
		# .[, value := frollmean(value, 30)] %>%
		na.omit(.)
	
	plot_test =
		list(
			cor_index %>%
				copy(.) %>%
				.[, series := 'Asset Contagion Index'] %>%
				.[, value := value - mean(value)],
			input_data %>%
				.[ticker == 'SPY'] %>%
				.[, c('date', 'value')] %>%
				.[, value := roll_mean(value, 30)/2] %>%
				.[, series := 'S&P 500'] %>%
				na.omit(.)
		) %>%
		rbindlist(.) %>%
		ggplot(.) + 
		geom_line(aes(x = date, y = value, color = series))
	
	print(plot_test)
	
	x_df =
		cor_values %>%
		.[window == 60] %>%
		.[, tickers := paste0(ticker_1, '_', ticker_2)] %>%
		dcast(., date ~ tickers, value.var = 'value') %>%
		na.omit(.)
	
	x_mat = as.matrix(select(x_df, -date))
	lambda_hat = eigen(t(x_mat) %*% x_mat)$vectors
	f_hat = (x_mat) %*% lambda_hat
	big_n = ncol(x_mat)
	big_t = nrow(x_mat)
	big_c_squared = min(big_n, big_t)
	
	# Total variance of data
	total_var = x_mat %>% cov(.) %>% diag(.) %>% sum(.)
	
	# Calculate ICs from Bai and Ng (2002)
	# Total SSE should be approx 0 due to normalization above;
	# sapply(1:ncol(xMat), function(i)
	# 	sapply(1:nrow(xMat), function(t)
	# 		(xMat[i, 1] - matrix(lambdaHat[i, ], nrow = 1) %*% matrix(fHat[t, ], ncol = 1))^2
	# 		) %>% sum(.)
	# 	) %>%
	# 	sum(.) %>%
	# 	{./(ncol(xMat) %*% nrow(xMat))}
	(x_mat - (f_hat %*% t(lambda_hat)))^1
	
	# Now test by R - first 100 only
	mse_by_r = sapply(1:100, function(r)
		sum((x_mat - (f_hat[, 1:r, drop = FALSE] %*% t(lambda_hat)[1:r, , drop = FALSE]))^2)/(big_t * big_n)
		)
	
	
	# Explained variance of data
	scree_df =
		f_hat %>% cov(.) %>% diag(.) %>%
		{lapply(1:100, function(i)
			tibble(
				factors = i,
				var_explained_by_factor = .[i],
				pct_of_total = .[i]/total_var,
				cum_pct_of_total = sum(.[1:i])/total_var
			)
		)} %>%
		bind_rows(.) %>%
		mutate(., mse = mse_by_r) %>%
		mutate(
			.,
			ic1 = (mse) + factors * (big_n + big_t)/(big_n * big_t) * log((big_n * big_t)/(big_n + big_t)),
			ic2 = (mse) + factors * (big_n + big_t)/(big_n * big_t) * log(big_c_squared),
			ic3 = (mse) + factors * (log(big_c_squared)/big_c_squared)
		)
	
	scree_plot =
		scree_df %>%
		ggplot(.) +
		geom_col(aes(x = factors, y = cum_pct_of_total, fill = factors)) +
		# geom_col(aes(x = factors, y = pct_of_total)) +
		labs(
			title = 'Percent of Variance Explained',
			x = 'Factors (R)', y = 'Cumulative % of Total Variance Explained', fill = NULL
		)
	
	big_r = 1
	
	z_df =
		x_df[, 'date'] %>%
		bind_cols(., f_hat[, 1:big_r] %>% as.data.frame(.) %>% set_names(., paste0('f', 1:big_r))) %>%
		.[, f1 := f1 * {if (tail(.[date <= '2019-01-01'], 1)$f1 >= 0) 1 else -1}]
	
	z_plots = imap(colnames(z_df) %>% .[. != 'date'], function(x, i)
		select(z_df, all_of(c('date', x))) %>%
			set_names(., c('date', 'value')) %>%
			ggplot(.) +
			geom_line(
				aes(x = date, y = value),
				color = hcl(h = seq(15, 375, length = big_r + 1), l = 65, c = 100)[i]
			) +
			labs(x = NULL, y = NULL, title = paste0('Estimated PCA Factor ', str_sub(x, -1), ' Plot')) +
			ggthemes::theme_fivethirtyeight() +
			scale_x_date(date_breaks = '1 year', date_labels = '%Y')
		)
	
	cor_index <<- cor_index
})
	
