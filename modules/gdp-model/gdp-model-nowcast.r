#'  Run this script on scheduler after close of business each day

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'GDP-MODEL-NOWCAST'
DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE
START_TIME = Sys.time()

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	sink(sink_conn, append = T, type = 'output')
	sink(sink_conn, append = T, type = 'message')
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(data.table)
library(httr)
library(DBI)
library(econforecasting)
library(lubridate)
library(jsonlite)
library(glmnet)

## Load Connection Info ----------------------------------------------------------
source(file.path(DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
	)
releases = list()
hist = list()
model = list()
models = list()

## Load Variable Defs ----------------------------------------------------------
variable_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'gdp-model-inputs')
release_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'data-releases')

## Set Backtest Dates  ----------------------------------------------------------
bdates = c(
	# Include all days in last 3 months
	seq(today() - days(30), today(), by = '1 day'),
	# Plus one random day per month before that
	seq(as_date('2020-01-01'), add_with_rollback(today() - days(30), months(-1)), by = '1 month') %>%
		map(., ~ sample(seq(floor_date(., 'month'), ceiling_date(., 'month'), '1 day'), 1))
	) %>% sort(.)

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message(str_glue('*** Getting Releases History: {round(as.numeric(difftime(now(), START_TIME)))} min'))

	fred_releases =
		variable_params %>%
		group_by(., relkey) %>%
		summarize(., n_varnames = n(), varnames = toJSON(fullname), .groups = 'drop') %>%
		left_join(
			.,
			variable_params  %>%
				filter(., nc_dfm_input == 1) %>%
				group_by(., relkey) %>%
				summarize(., n_dfm_varnames = n(), dfm_varnames = toJSON(fullname), .groups = 'drop'),
			variable_params %>%
				group_by(., relkey) %>%
				summarize(., n_varnames = n(), varnames = toJSON(fullname), .groups = 'drop'),
			by = 'relkey'
		) %>%
		left_join(., release_params, by = 'relkey') %>%
		# Now create a column of included releaseDates
		left_join(
			.,
			map_dfr(purrr::transpose(filter(., relsc == 'fred')), function(x) {

				RETRY(
					'GET',
					str_glue(
						'https://api.stlouisfed.org/fred/release/dates?',
						'release_id={x$relsckey}&realtime_start=2020-01-01',
						'&include_release_dates_with_no_data=true&api_key={CONST$FRED_API_KEY}&file_type=json'
					),
					times = 10
					) %>%
					content(., as = 'parsed') %>%
					.$release_dates %>%
					sapply(., function(y) y$date) %>%
					tibble(relkey = x$relkey, reldates = .)
				}) %>%
				group_by(., relkey) %>%
				summarize(., reldates = jsonlite::toJSON(reldates), .groups = 'drop'),
			by = 'relkey'
			)

	releases$raw$fred <<- fred_releases
})

## 2. Combine ----------------------------------------------------------
local({

	releases$final <<- bind_rows(releases$raw)
})

# Historical Data ----------------------------------------------------------

## 1. FRED ----------------------------------------------------------
local({

	message('*** Importing FRED Data')

	fred_data =
		variable_params %>%
		purrr::transpose(.) %>%
		keep(., ~ .$source == 'fred') %>%
		imap_dfr(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_data(x$sckey, CONST$FRED_API_KEY, .freq = x$freq, .return_vintages = T, .verbose = F) %>%
				transmute(., varname = x$varname, freq = x$freq, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date('2010-01-01'), vdate >= as_date('2010-01-01'))
			})

	hist$raw$fred <<- fred_data
})

## 2. Yahoo Finance ----------------------------------------------------------
local({

	message('*** Importing Yahoo Finance Data')

	yahoo_data =
		variable_params %>%
		purrr::transpose(.) %>%
		keep(., ~ .$source == 'yahoo') %>%
		map_dfr(., function(x) {
			url =
				paste0(
					'https://query1.finance.yahoo.com/v7/finance/download/', x$sckey,
					'?period1=', '1262304000', # 12/30/1999
					'&period2=', as.numeric(as.POSIXct(Sys.Date() + lubridate::days(1))),
					'&interval=1d',
					'&events=history&includeAdjustedClose=true'
				)
			data.table::fread(url, showProgress = FALSE) %>%
				.[, c('Date', 'Adj Close')]	%>%
				set_names(., c('date', 'value')) %>%
				as_tibble(.) %>%
				# Bug with yahoo finance returning null for date 7/22/21 as of 7/23
				filter(., value != 'null') %>%
				mutate(
					.,
					varname = x$varname,
					freq = x$freq,
					date,
					vdate = date,
					value = as.numeric(value)
					) %>%
				return(.)
		})

	hist$raw$yahoo <<- yahoo_data
})

## 3. Calculated Variables ----------------------------------------------------------
local({

	message('*** Adding Calculated Variables')

	fred_data =
		hist$raw$fred %>%
		filter(., freq == 'd' & (str_detect(varname, 't\\d{2}[m|y]') | varname == 'ffr'))

	# Monthly aggregation & append EOM with current val
	fred_data_cat =
		fred_data %>%
		mutate(., date = floor_date(date, 'months')) %>%
		group_by(., varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop')

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		variable_params %>%
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

	dns_data =
		dns_fit_hist %>%
		group_by(., date) %>%
		summarize(., tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3)) %>%
		pivot_longer(., -date, names_to = 'varname', values_to = 'value') %>%
		transmute(
			.,
			varname,
			freq = 'm',
			date,
			vdate =
				# Avoid storing too much old DNS data - assign vdate as newesty possible value of edit for
				# previous months
				as_date(ifelse(ceiling_date(date, 'months') <= today(), ceiling_date(date, 'months'), today())),
			value = as.numeric(value)
		)

	# For each vintage date, pull all CPI values from the latest available vintage for each date
	# This intelligently handles revisions and prevents
	# duplications of CPI values by vintage dates/obs date
	cpi_df =
		hist$raw$fred %>%
		filter(., varname == 'cpi' & freq == 'm') %>%
		select(., date, vdate, value)

	inf_data =
		cpi_df %>%
		group_split(., vdate) %>%
		purrr::map_dfr(., function(x)
			filter(cpi_df, vdate <= x$vdate[[1]]) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				transmute(date, vdate = roll::roll_max(vdate, 13), value = 100 * (value/lag(value, 12) - 1))
			) %>%
		na.omit(.) %>%
		distinct(., date, vdate, value) %>%
		transmute(
			.,
			varname = 'inf',
			freq = 'm',
			date,
			vdate,
			value
		)

	# Calculate misc interest rate spreads from cross-weekly/daily rates
	# Assumes no revisions for this data
	rates_wd_data = tribble(
		~ varname, ~ var_w, ~ var_d,
		'mort30yt30yspread', 'mort30y', 't30y',
		'mort15yt10yspread', 'mort15y', 't10y'
		) %>%
		purrr::transpose(., .names = .$varname) %>%
		map_dfr(., function(x) {

			# Use weekly data as mortgage pulls are weekly
			var_w_df =
				filter(hist$raw$fred, varname == x$var_w & freq == 'w') %>%
				select(., varname, date, vdate, value)

			var_d_df =
				filter(hist$raw$fred, varname == x$var_d & freq == 'd') %>%
				select(., varname, date, vdate, value)

			# Get week-end days
			w_obs_dates =
				filter(hist$raw$fred, varname == x$var_w & freq == 'w') %>%
				.$date %>%
				unique(.)

			# Iterate through the week-end days
			purrr::map_dfr(w_obs_dates, function(w_obs_date) {

				var_d_df_x =
					var_d_df %>%
					filter(., date %in% seq(w_obs_date - days(6), to = w_obs_date, by = '1 day')) %>%
					summarize(., d_vdate = max(vdate), d_value = mean(value))

				var_w_df_x =
					var_w_df %>%
					filter(., date == w_obs_date) %>%
					transmute(., w_vdate = vdate, w_value = value)

				if (nrow(var_d_df_x) == 0 | nrow(var_w_df_x) == 0) return(NA)

				bind_cols(var_w_df_x, var_d_df_x) %>%
					transmute(
						.,
						varname = x$varname,
						freq = 'w',
						date = w_obs_date,
						vdate = max(w_vdate, d_vdate),
						value = w_value - d_value
						) %>%
					return(.)

				}) %>%
				distinct(.)
			})

	hist_calc = bind_rows(dns_data, inf_data, rates_wd_data)

	hist$raw$calc <<- hist_calc
})

## 4. Aggregate Frequencies ----------------------------------------------------------
local({

	message('*** Aggregating Monthly & Quarterly Data')

	hist_agg_0 = bind_rows(hist$raw)

	monthly_agg =
		# Get all daily/weekly varnames with pre-existing data
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w')) %>%
		# Add in month of date
		mutate(., this_month = lubridate::floor_date(date, 'month')) %>%
		as.data.table(.) %>%
		# For each variable, for each month, create a dataframe of all month obs across all vintages
		# Then for each vintage date, take the latest vintage date for every obs in the month and
		# calculate the rolling mean
		split(., by = c('this_month', 'varname')) %>%
		lapply(., function(x)
			x %>%
				dcast(., vdate ~ date, value.var = 'value') %>%
				.[order(vdate)] %>%
				.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
				# {data.table(vdate = .$vdate, value = rowMeans(.[, -1], na.rm = T))} %>%
				melt(., id.vars = 'vdate', value.name = 'value', variable.name = 'input_date', na.rm = T) %>%
				.[, input_date := as_date(input_date)] %>%
				.[, list(value = mean(value, na.rm = T), count = .N), by = 'vdate'] %>%
				.[, c('varname', 'date') := list(x$varname[[1]], date = x$this_month[[1]])]
		) %>%
		rbindlist(.) %>%
		as_tibble(.) %>%
		transmute(., varname, freq = 'm', date, vdate, value)


	# Works similarly as monthly aggregation but does not create new quarterly data unless all
	# 3 monthly data points are available, and where the vintage is at least as great as the
	# EOQ value of the obs date
	quarterly_agg =
		monthly_agg %>%
		bind_rows(., filter(hist_agg_0, freq == 'm')) %>%
		filter(., varname == 'spy') %>%
		mutate(., this_quarter = lubridate::floor_date(date, 'quarter')) %>%
		as.data.table(.) %>%
		split(., by = c('this_quarter', 'varname')) %>%
		lapply(., function(x)
			x %>%
				dcast(., vdate ~ date, value.var = 'value') %>%
				.[order(vdate)] %>%
				.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
				melt(., id.vars = 'vdate', value.name = 'value', variable.name = 'input_date', na.rm = T) %>%
				.[, input_date := as_date(input_date)] %>%
				.[, list(value = mean(value, na.rm = T), count = .N), by = 'vdate'] %>%
				.[, c('varname', 'date') := list(x$varname[[1]], date = x$this_quarter[[1]])] #%>%
				# .[count == 3]
			) %>%
		rbindlist(.) %>%
		as_tibble(.) %>%
		transmute(., varname, freq = 'q', date, vdate, value)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q'))

	hist$agg <<- hist_agg
})

## 5. Split By Vintage Date ----------------------------------------------------------
local({

	message('*** Splitting by Vintage Date')

	# Check min dates
	# The latest min date for PCA inputs should be no earlier than the last PCA input variable
	hist$agg %>%
		group_by(., varname) %>%
		summarize(., min_dt = min(date)) %>%
		arrange(., desc(min_dt)) %>%
		print(., n = 5)

	last_obs_by_vdate =
		hist$agg %>%
		as.data.table(.) %>%
		split(., by = c('varname', 'freq')) %>%
		lapply(., function(x)  {

			message(str_glue('... Getting last vintage dates for {x$varname[[1]]}'))

			last_obs_for_all_vdates =
				x %>%
					.[order(vdate)] %>%
					dcast(., varname + freq + vdate ~  date, value.var = 'value') %>%
					.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
					melt(
						.,
						id.vars = c('varname', 'freq', 'vdate'),
						value.name = 'value',
						variable.name = 'date',
						na.rm = T
					)

			lapply(bdates, function(test_vdate)
				last_obs_for_all_vdates %>%
					.[vdate <= test_vdate] %>%
					{if (nrow(.) == 0) NULL else .[vdate == max(vdate)] %>% .[, bdate := test_vdate]}
				) %>%
				rbindlist(.)
		}) %>%
		rbindlist(.)

	hist$base <<- last_obs_by_vdate
})


## 6. Add Stationary Transformations ----------------------------------------------------------
local({

	message('*** Adding Stationary Transformations')

	stat_groups =
		hist$base %>%
		split(., by = c('varname', 'freq', 'vdate')) %>%
		unname(.)

	stat_final =
		stat_groups %>%
		imap(., function(x, i) {

			if (i %% 1000 == 0) message(str_glue('... Transforming {i} of {length(stat_groups)}'))

			variable_param = purrr::transpose(filter(variable_params, varname == x$varname[[1]]))[[1]]
			if (is.null(variable_param)) stop(str_glue('Missing {x$varname[[1]]} in variable_params'))

			last_obs_by_vdate_t = lapply(c('st', 'd1', 'd2'), function(this_form) {
				transform = variable_param[[this_form]]
				copy(x) %>%
					.[,
						value := {
							if (transform == 'none') NA
							else if (transform == 'base') value
							else if (transform == 'dlog') dlog(value)
							else if (transform == 'diff1') diff1(value)
							else if (transform == 'pchg') pchg(value)
							else if (transform == 'apchg') apchg(value, {if (variable_param$freq == 'q') 4 else 12})
							else stop('Error')
						}] %>%
					.[, form := this_form]
				}) %>%
				rbindlist(.)
		}) %>%
		rbindlist(.) %>%
		bind_rows(., hist$base[, form := 'base']) %>%
		na.omit(.)

	stat_final_last = stat_final[bdate == max(bdate)]

	hist$flat <<- stat_final
	hist$flat_last <<- stat_final_last
})

## 7. Create Monthly/Quarterly Matrices ----------------------------------------------------------
local({

	wide =
		hist$flat %>%
		split(., by = 'freq', keep.by = F) %>%
		lapply(., function(x)
			split(x, by = 'form', keep.by = F) %>%
				lapply(., function(y)
					split(y, by = 'bdate', keep.by = F) %>%
					lapply(., function(z)
						dcast(z, date ~ varname, value.var = 'value') %>%
							.[, date := as_date(date)] %>%
							.[order(date)] %>%
							as_tibble(.)
						)
				)
			)

	wide_last <<- lapply(wide, function(x) lapply(x, function(y) tail(y, 1)[[1]]))

	hist$wide <<- wide
	hist$wide_last <<- wide_last
})

# State-Space Model -----------------------------------------------------------------

## 1. Dates -----------------------------------------------------------------
local({

	quarters_forward = 4
	pca_varnames = filter(variable_params, nc_dfm_input == T)$varname

	results = lapply(bdates, function(this_bdate) {
	
		pca_variables_df =
			hist$wide$m$st[[as.character(this_bdate)]] %>%
			select(., date, all_of(pca_varnames)) %>%
			filter(., date >= as_date('2012-01-01'))

		big_t_dates = filter(pca_variables_df, !if_any(everything(), is.na))$date
		big_tau_dates = filter(pca_variables_df, if_any(everything(), is.na))$date
		big_tstar_dates =
			big_tau_dates %>%
			tail(., 1) %>%
			seq(
				from = .,
				to =
					# From this quarter to next quarter minus a month
					from_pretty_date(paste0(lubridate::year(.), 'Q', lubridate::quarter(.)), 'q') %>%
					lubridate::add_with_rollback(., months(3 * (1 + quarters_forward) - 1)),
				by = '1 month'
			) %>%
			.[2:length(.)]

		big_t_date = tail(big_t_dates, 1)
		big_tau_date = tail(big_tau_dates, 1)
		big_tstar_date = tail(big_tstar_dates, 1)

		big_t = length(big_t_dates)
		big_tau = big_t + length(big_tau_dates)
		big_tstar = big_tau + length(big_tstar_dates)

		time_df = tibble(
			date = as_date(c(pca_variables_df$date[[1]], big_t_date, big_tau_date, big_tstar_date)),
			time = c('1', 'T', 'Tau', 'T*')
			)

		list(
			bdate = this_bdate,
			pca_variables_df = pca_variables_df,
			big_t_dates = big_t_dates,
			big_tau_dates = big_tau_dates,
			big_tstar_dates = big_tstar_dates,
			big_t_date = big_t_date,
			big_tau_date = big_tau_date,
			big_tstar_date = big_tstar_date,
			big_t = big_t,
			big_tau = big_tau,
			big_tstar = big_tstar,
			time_df = time_df
			)
		})
	
	
	for (x in results) {
		models[[as.character(x$bdate)]] <<- x
	}
	model$quarters_forward <<- quarters_forward
	model$pca_varnames <<- pca_varnames
})


## 2. Extract PCA Factors -----------------------------------------------------------------
local({
	
	results = lapply(bdates, function(this_bdate) {

		m = models[[as.character(this_bdate)]]
		
		xDf = filter(m$pca_variables_df, date %in% m$big_t_dates)
	
		xMat = scale(as.matrix(select(xDf, -date)))
	
		lambdaHat = eigen(t(xMat) %*% xMat)$vectors
		fHat = (xMat) %*% lambdaHat
		bigN = ncol(xMat)
		bigT = nrow(xMat)
		bigCSquared = min(bigN, bigT)
	
		# Total variance of data
		totalVar = xMat %>% cov(.) %>% diag(.) %>% sum(.)
	
		# Calculate ICs from Bai and Ng (2002)
		# Total SSE should be approx 0 due to normalization above;
		# sapply(1:ncol(xMat), function(i)
		# 	sapply(1:nrow(xMat), function(t)
		# 		(xMat[i, 1] - matrix(lambdaHat[i, ], nrow = 1) %*% matrix(fHat[t, ], ncol = 1))^2
		# 		) %>% sum(.)
		# 	) %>%
		# 	sum(.) %>%
		# 	{./(ncol(xMat) %*% nrow(xMat))}
		(xMat - (fHat %*% t(lambdaHat)))^1
	
		# Now test by R
		mseByR =
			sapply(1:bigN, function(r)
				sum((xMat - (fHat[, 1:r, drop = FALSE] %*% t(lambdaHat)[1:r, , drop = FALSE]))^2)/(bigT * bigN)
			)
	
	
		# Explained variance of data
		screeDf =
			fHat %>% cov(.) %>% diag(.) %>%
			{lapply(1:length(.), function(i)
				tibble(
					factors = i,
					var_explained_by_factor = .[i],
					pct_of_total = .[i]/totalVar,
					cum_pct_of_total = sum(.[1:i])/totalVar
				)
			)} %>%
			dplyr::bind_rows(.) %>%
			dplyr::mutate(., mse = mseByR) %>%
			dplyr::mutate(
				.,
				ic1 = (mse) + factors * (bigN + bigT)/(bigN * bigT) * log((bigN * bigT)/(bigN + bigT)),
				ic2 = (mse) + factors * (bigN + bigT)/(bigN * bigT) * log(bigCSquared),
				ic3 = (mse) + factors * (log(bigCSquared)/bigCSquared)
			)
	
		screePlot =
			screeDf %>%
			ggplot(.) +
			geom_col(aes(x = factors, y = cum_pct_of_total, fill = factors)) +
			# geom_col(aes(x = factors, y = pct_of_total)) +
			labs(
				title = 'Percent of Variance Explained',
				x = 'Factors (R)', y = 'Cumulative % of Total Variance Explained', fill = NULL
				)

		bigR =
			screeDf %>%
			dplyr::filter(., ic1 == min(ic1)) %>%
			.$factors + 0#+ 2
		# ((
		# 	{screeDf %>% dplyr::filter(cum_pct_of_total >= .80) %>% head(., 1) %>%.$factors} +
		#   	{screeDf %>% dplyr::filter(., ic1 == min(ic1)) %>% .$factors}
		# 	)/2) %>%
		# round(., digits = 0)
	
		zDf =
			xDf[, 'date'] %>%
			dplyr::bind_cols(
				.,
				fHat[, 1:bigR] %>% as.data.frame(.) %>% setNames(., paste0('f', 1:bigR))
			)
	
	
		zPlots =
			purrr::imap(colnames(zDf) %>% .[. != 'date'], function(x, i)
				dplyr::select(zDf, all_of(c('date', x))) %>%
					setNames(., c('date', 'value')) %>%
					ggplot() +
					geom_line(
						aes(x = date, y = value),
						color = hcl(h = seq(15, 375, length = bigR + 1), l = 65, c = 100)[i]
					) +
					labs(x = NULL, y = NULL, title = paste0('Estimated PCA Factor ', str_sub(x, -1), ' Plot')) +
					ggthemes::theme_fivethirtyeight() +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y')
			)
	
		factorWeightsDf =
			lambdaHat %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			setNames(., paste0('f', str_pad(1:ncol(.), pad = '0', 1))) %>%
			dplyr::bind_cols(weight = colnames(xMat), .) %>%
			tidyr::pivot_longer(., -weight, names_to = 'varname') %>%
			dplyr::group_by(varname) %>%
			dplyr::arrange(., varname, desc(abs(value))) %>%
			dplyr::mutate(., order = 1:n(), valFormat = paste0(weight, ' (', round(value, 2), ')')) %>%
			dplyr::ungroup(.) %>%
			dplyr::select(., -value, -weight) %>%
			tidyr::pivot_wider(., names_from = varname, values_from = valFormat) %>%
			dplyr::arrange(., order) %>%
			dplyr::select(., -order) %>%
			dplyr::select(., paste0('f', 1:bigR))
		
		list(
			bdate = this_bdate,
			factor_weights_df = factorWeightsDf,
			scree_df = screeDf,
			scree_plot = screePlot,
			big_r = bigR,
			pca_input_df = xDf,
			z_df = zDf,
			z_plots = zPlots
		)
	})
	
	

	for (x in results) {
		models[[as.character(x$bdate)]]$factor_weights_df <<- x$factor_weights_df
		models[[as.character(x$bdate)]]$scree_df <<- x$scree_df
		models[[as.character(x$bdate)]]$scree_plot <<- x$scree_plot
		models[[as.character(x$bdate)]]$big_r <<- x$big_r
		models[[as.character(x$bdate)]]$pca_input_df <<- x$pca_input_df
		models[[as.character(x$bdate)]]$z_df <<- x$z_df
		models[[as.character(x$bdate)]]$z_plots <<- x$z_plots
	}
})

## 3. Run as VAR(1) -----------------------------------------------------------------
local({
	
	message('*** Running VAR(1)')
	
	results = lapply(bdates, function(this_bdate) {
		
		m = models[[as.character(this_bdate)]]
		
		input_df = na.omit(inner_join(m$z_df, add_lagged_columns(m$z_df, max_lag = 1), by = 'date'))

		y_mat = input_df %>% select(., -contains('.l'), -date) %>% as.matrix(.)
		x_df = input_df %>% select(., contains('.l')) %>% bind_cols(constant = 1, .)
			
		coef_df =
			lm(y_mat ~ . - 1, data = x_df) %>%
			coef(.) %>%
			as.data.frame(.) %>%
			rownames_to_column(., 'coefname') %>%
			as_tibble(.) %>%
			set_names(., c('coefname', paste0('f', 1:m$big_r)))
			
		gof_df =
			lm(y_mat ~ . - 1, x_df) %>%
			resid(.) %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			pivot_longer(., everything(), names_to = 'varname') %>%
			group_by(., varname) %>%
			summarize(., MAE = mean(abs(value)), MSE = mean(value^2))
			
		resid_plot =
			lm(y_mat ~ . - 1, x_df) %>%
			resid(.) %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			bind_cols(date = input_df$date, .) %>%
			pivot_longer(., -date) %>%
			ggplot(.) + 	
			geom_line(aes(x = date, y = value, group = name, color = name), size = 1) +
			labs(title = 'Residuals plot for PCA factors', x = NULL, y = NULL, color = NULL)

		fitted_plots =
			lm(y_mat ~ . - 1, data = x_df) %>%
			fitted(.) %>%
			as_tibble(.) %>%
			bind_cols(date = input_df$date, ., type = 'Fitted Values') %>%
			bind_rows(., m$z_df %>% dplyr::mutate(., type = 'Data')) %>%
			pivot_longer(., -c('type', 'date')) %>% 
			as.data.table(.) %>%
			split(., by = 'name') %>%
			imap(., function(x, i)
				ggplot(x) +
					geom_line(
						aes(x = date, y = value, group = type, linetype = type, color = type),
						size = 1, alpha = 1.0
					) +
					labs(
						x = NULL, y = NULL, color = NULL, linetype = NULL,
						title = paste0('Fitted values vs actual for factor ', i)
					) +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y') +
					ggthemes::theme_fivethirtyeight()
				)
			
			b_mat = coef_df %>% filter(., coefname != 'constant') %>% select(., -coefname) %>% t(.)
			c_mat = coef_df %>% filter(., coefname == 'constant') %>% select(., -coefname) %>% t(.)
			q_mat =
				lm(y_mat ~ . - 1, data = x_df) %>%
				residuals(.) %>% 
				as_tibble(.) %>%
				purrr::transpose(.) %>%
				lapply(., function(x) as.numeric(x)^2 %>% diag(., nrow = length(.), ncol = length(.))) %>%
				{reduce(., function(x, y) x + y)/length(.)}
			
			list(
				bdate = this_bdate,
				var_fitted_plots = fitted_plots,
				var_resid_plots = resid_plot,
				var_gof_df = gof_df,
				var_coef_df = coef_df,
				q_mat = q_mat,
				b_mat = b_mat,
				c_mat = c_mat
			)
		})
	
	for (x in results) {
		models[[as.character(x$bdate)]]$var_fitted_plots <<- x$var_fitted_plots
		models[[as.character(x$bdate)]]$var_resid_plots <<- x$var_resid_plots
		models[[as.character(x$bdate)]]$var_gof_df <<- x$var_gof_df
		models[[as.character(x$bdate)]]$var_coef_df <<- x$var_coef_df
		models[[as.character(x$bdate)]]$q_mat <<- x$q_mat
		models[[as.character(x$bdate)]]$b_mat <<- x$b_mat
		models[[as.character(x$bdate)]]$c_mat <<- x$c_mat
	}
})

## 4. Run DFM on PCA Monthly Vars -----------------------------------------------------------------
local({
	
	results = lapply(bdates, function(this_bdate) {
		
		m = models[[as.character(this_bdate)]]
		
		y_mat = as.matrix(select(m$pca_input_df, -date))
		x_df = bind_cols(constant = 1, select(m$z_df, -date))
		
		coef_df =
			lm(y_mat ~ . - 1, x_df) %>%
			coef(.) %>%	
			as.data.frame(.) %>%
			rownames_to_column(., 'coefname') %>%
			as_tibble(.)
		
		
		fitted_plots =
			lm(y_mat ~ . - 1, x_df) %>%
			fitted(.) %>%
			as_tibble(.) %>%
			bind_cols(date = m$z_df$date, ., type = 'Fitted Values') %>%
			bind_rows(., mutate(m$pca_input_df, type = 'Data')) %>%
			pivot_longer(., -c('type', 'date')) %>%
			as.data.table(.) %>%
			split(., by = 'name') %>%
			purrr::imap(., function(x, i)
				ggplot(x) +
					geom_line(
						aes(x = date, y = value, group = type, linetype = type, color = type),
						size = 1, alpha = 1.0
					) +
					labs(
						x = NULL, y = NULL, color = NULL, linetype = NULL,
						title = paste0('Fitted values vs actual for factor ', i)
					) +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y')
			)
		
		gof_df =
			lm(y_mat ~ . - 1, x_df) %>%
			resid(.) %>%
			as.data.frame(.) %>%
			as_tibble(.) %>%
			pivot_longer(., everything(), names_to = 'varname') %>%
			group_by(., varname) %>%
			summarize(., MAE = mean(abs(value)), MSE = mean(value^2))
		
		
		a_mat = coef_df %>% filter(., coefname != 'constant') %>% select(., -coefname) %>% t(.)
		d_mat = coef_df %>% filter(., coefname == 'constant') %>% select(., -coefname) %>% t(.)
		
		r_mat_0 =
			lm(y_mat ~ . - 1, data = x_df) %>%
			residuals(.) %>% 
			as_tibble(.) %>%
			purrr::transpose(.) %>%
			lapply(., function(x) as.numeric(x)^2 %>% diag(.)) %>%
			{purrr::reduce(., function(x, y) x + y)/length(.)}
		
		r_mat_diag = tibble(varname = model$pca_varnames, variance = diag(r_mat_0))
		
		r_mats =
			lapply(m$big_tau_dates, function(d)
				sapply(model$pca_varnames, function(v)
					hist$wide$m$st[[as.character(this_bdate)]] %>%
						filter(., date == (d)) %>%
						.[[v]] %>% 
						{if (is.na(.)) 1e20 else filter(r_mat_diag, varname == v)$variance}
					) %>%
					diag(.)
			) %>%
			c(lapply(1:length(m$big_t_dates), function(x) r_mat_0), .)
		
		list(
			bdates = this_bdate,
			dfm_gof_df = gof_df,
			dfm_coef_df = coef_df,
			dfm_fitted_plots = fitted_plots,
			r_mats = r_mats,
			a_mat = a_mat,
			d_mat = d_mat
		)
	})
	
	
	
	for (x in results) {
		models[[as.character(x$bdate)]]$dfm_gof_df <<- x$dfm_gof_df
		models[[as.character(x$bdate)]]$dfm_coef_df <<- x$dfm_coef_df
		models[[as.character(x$bdate)]]$dfm_fitted_plots <<- x$dfm_fitted_plots
		models[[as.character(x$bdate)]]$r_mats <<- x$r_mats
		models[[as.character(x$bdate)]]$a_mat <<- x$a_mat
		models[[as.character(x$bdate)]]$d_mat <<- x$d_mat
	}
})



## 5. Kalman Filter on State Space Obs -----------------------------------------------------------------
local({

	results = lapply(bdates, function(this_bdate) {
		
		m = models[[as.character(this_bdate)]]
		
		b_mat = m$b_mat
		c_mat = m$c_mat
		a_mat = m$a_mat
		d_mat = m$d_mat
		r_mats = m$r_mats
		q_mat = m$q_mat
		y_mats =
			bind_rows(
				m$pca_input_df,
				hist$wide$m$st[[as.character(this_bdate)]] %>%
					filter(., date %in% m$big_tau_dates) %>%
					select(., date, model$pca_varnames)
			) %>%
			mutate(., across(-date, function(x) ifelse(is.na(x), 0, x))) %>%
			select(., -date) %>%
			purrr::transpose(.) %>%
			lapply(., function(x) matrix(unlist(x), ncol = 1))
		
		z0Cond0 = matrix(rep(0, m$big_r), ncol = 1)
		sigmaZ0Cond0 = matrix(rep(0, m$big_r^2), ncol = m$big_r)
		
		zTCondTMinusOne = list()
		zTCondT = list()
		
		sigmaZTCondTMinusOne = list()
		sigmaZTCondT = list()
		
		yTCondTMinusOne = list()
		sigmaYTCondTMinusOne = list()
		
		pT = list()
		
		## Filter Step
		for (t in 1:length(c(m$big_t_dates, m$big_tau_dates))) {
			# message(t)
			# Prediction Step
			zTCondTMinusOne[[t]] = b_mat %*% {if (t == 1) z0Cond0 else zTCondT[[t-1]]} + c_mat
			sigmaZTCondTMinusOne[[t]] = b_mat %*% {if (t == 1) sigmaZ0Cond0 else sigmaZTCondT[[t-1]]} + q_mat
			yTCondTMinusOne[[t]] = a_mat %*% zTCondTMinusOne[[t]] + d_mat
			sigmaYTCondTMinusOne[[t]] = a_mat %*% sigmaZTCondTMinusOne[[t]] %*% t(a_mat) + r_mats[[t]]
			
			# Correction Step
			pT[[t]] = sigmaZTCondTMinusOne[[t]] %*% t(a_mat) %*%
				{
					if (t %in% 1:length(m$big_t_dates)) solve(sigmaYTCondTMinusOne[[t]])
					else chol2inv(chol(sigmaYTCondTMinusOne[[t]]))
				}
			zTCondT[[t]] = zTCondTMinusOne[[t]] + pT[[t]] %*% (y_mats[[t]] - yTCondTMinusOne[[t]])
			sigmaZTCondT[[t]] = sigmaZTCondTMinusOne[[t]] - (pT[[t]] %*% sigmaYTCondTMinusOne[[t]] %*% t(pT[[t]]))
		}
		
		kFitted =
			zTCondT %>%
			purrr::map_dfr(., function(x)
				as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
			bind_cols(date = c(m$big_t_dates, m$big_tau_dates), .) 
		
		
		## Smoothing step
		zTCondBigTSmooth = list()
		sigmaZTCondBigTSmooth = list()
		sT = list()
		
		for (t in (length(zTCondT) - 1): 1) {
			# message(t)
			sT[[t]] = sigmaZTCondT[[t]] %*% t(b_mat) %*% solve(sigmaZTCondTMinusOne[[t + 1]])
			zTCondBigTSmooth[[t]] = zTCondT[[t]] + sT[[t]] %*% 
				({if (t == length(zTCondT) - 1) zTCondT[[t + 1]] else zTCondBigTSmooth[[t + 1]]} - zTCondTMinusOne[[t + 1]])
			sigmaZTCondBigTSmooth[[t]] = sigmaZTCondT[[t]] - sT[[t]] %*%
				(sigmaZTCondTMinusOne[[t + 1]] -
				 	{if (t == length(zTCondT) - 1) sigmaZTCondT[[t + 1]] else sigmaZTCondBigTSmooth[[t + 1]]}
				) %*% t(sT[[t]])
		}
		
		kSmooth =
			zTCondBigTSmooth %>%
			map_dfr(., function(x)
				as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
			bind_cols(date = c(m$big_t_dates, m$big_tau_dates) %>% .[1:(length(.) - 1)], .) 
		
		
		
		## Forecasting step
		zTCondBigT = list()
		sigmaZTCondBigT = list()
		yTCondBigT = list()
		sigmaYTCondBigT = list()
		
		for (j in 1:length(m$big_tstar_dates)) {
			zTCondBigT[[j]] = b_mat %*% {if (j == 1) zTCondT[[length(zTCondT)]] else zTCondBigT[[j - 1]]} + c_mat
			sigmaZTCondBigT[[j]] = b_mat %*%
				{if (j == 1) sigmaZTCondT[[length(sigmaZTCondT)]] else sigmaZTCondBigT[[j - 1]]} + q_mat
			yTCondBigT[[j]] = a_mat %*% zTCondBigT[[j]] + d_mat
			sigmaYTCondBigT[[j]] = a_mat %*% sigmaZTCondBigT[[j]] %*% t(a_mat) + r_mats[[1]]
		}
		
		kForecast =
			zTCondBigT %>%
			map_dfr(., function(x)
				as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
			bind_cols(date = m$big_tstar_dates, .)
		
		
		
		## Plot and Cleaning
		kf_plots =
			lapply(colnames(m$z_df) %>% .[. != 'date'], function(.varname)
				bind_rows(
					mutate(m$z_df, type = 'Data'),
					mutate(kFitted, type = 'Kalman Filtered'),
					mutate(kForecast, type = 'Forecast'),
					mutate(kSmooth, type = 'Kalman Smoothed'),
				) %>%
					pivot_longer(., -c('date', 'type'), names_to = 'varname') %>%
					filter(., varname == .varname) %>%
					ggplot(.) +
					geom_line(aes(x = date, y = value, color = type), size = 1) + 
					labs(x = NULL, y = NULL, color = NULL, title = paste0('Kalman smoothed values for ', .varname)) +
					scale_x_date(date_breaks = '1 year', date_labels = '%Y')
			)
		
		
		f_df = bind_rows(
			kSmooth,
			tail(kFitted, 1),
			kForecast # %>% mutate(., f1 = mean(filter(m$z_df, date < '2020-03-01')$f1))
			)
		
		y_df =
			yTCondBigT %>%
			map_dfr(., function(x) as_tibble(t(as.data.frame(x)))) %>%
			bind_cols(date = m$big_tstar_dates, .)
		
		kf_df = y_df
		
		list(
			bdates = this_bdate,
			f_df = f_df,
			kf_plots = kf_plots,
			y_df = y_df,
			kf_df = kf_df
			)
	})
	
	for (x in results) {
		models[[as.character(x$bdate)]]$f_df <<- x$f_df
		models[[as.character(x$bdate)]]$kf_plots <<- x$kf_plots
		models[[as.character(x$bdate)]]$y_df <<- x$y_df
		models[[as.character(x$bdate)]]$kf_df <<- x$kf_df
	}
})


# Nowcast  -----------------------------------------------------------------

## 1. Monthly Variables (DFM-AR)  -----------------------------------------------------------------
#' TBD: Forecast by rewriting to VAR(p)
#' y_{t+s} = A^s*Y_t + sum^{s-1}_{j=0} (A^j (BF_{t+j} + C))
local({

	message('*** Forecasting Monthly Variables')
	
	dfm_varnames = filter(variable_params, nc_method == 'dfm.m')$varname
	ar_lags = 1 # Lag can be included from 1-4

	# As of 2/6/22 
	# Single core takes about 1s per date for ar_lags = 2
	results = lapply(bdates, function(this_bdate) {
		
		message(str_glue('... Forecasting {this_bdate}'))
		m = models[[as.character(this_bdate)]]

		# Only include variables which are available at that date
		stat_df =
			hist$wide$m$st[[as.character(this_bdate)]] %>%
			select(., all_of(c('date', dfm_varnames[dfm_varnames %in% colnames(.)]))) %>%
			# Add lags & inner join F
			{if (ar_lags > 0) bind_cols(., select(add_lagged_columns(., max_lag = ar_lags), -date)) else .} %>%
			inner_join(., m$f_df, by = 'date') %>%
			as.data.table(.)
		
		vars_to_forecast = colnames(stat_df) %>% keep(., ~ . %in% dfm_varnames)
		factor_vars = paste0('f', 1:m$big_r)
		
		dfm_df = lapply(vars_to_forecast, function(.varname) {
			
			# message(.varname)
			lag_vars = {if (ar_lags == 0) NULL else paste0(.varname, '.l', 1:ar_lags)}

			# Get inputs - include all historical data
			input_df =
				stat_df[, c('date', .varname, factor_vars, lag_vars), with = F] %>%
				na.omit(.)

			y_mat = input_df[, c(.varname), with = F] %>% as.matrix(.)
			x_mat = input_df[, c(factor_vars, lag_vars), with = F][, constant := 1] %>% as.matrix(.)
			
			coef_df =
				(solve(t(x_mat) %*% x_mat) %*% (t(x_mat) %*% y_mat)) %>%
				as.data.table(.) %>%
				.[, coefname := c(factor_vars, lag_vars, 'constant')] %>%
				set_names(., c('value', 'coefname'))
			
			# Initialize lag Y_0 matrix
			y_0 =
				stat_df[order(-date)][date <= tail(input_df$date, 1), .varname, with = F] %>%
				head(., ar_lags) %>%
				as.matrix(., ncol = 1) 
			
			# Initialize factor matrix
			f_mats =
				as.data.table(m$f_df)[date > tail(input_df$date, 1), !'date'] %>%
				split(., 1:nrow(.)) %>%
				lapply(., function(x) t(x))
			
			# Initialize factor loadings matrix
			b_mat =
				coef_df[coefname %chin% factor_vars]$value %>%
				matrix(., nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., matrix(rep(0, (ar_lags - 1) * length(factor_vars)), ncol = length(factor_vars)))
					}
			
			# Initialize constant matrix
			c_mat = 
				c(coef_df[coefname == 'constant']$value, {if (ar_lags == 0) NULL else rep(0, ar_lags - 1)}) %>%
				matrix(., ncol = 1)
			
			# Lag weighted matrix
			a_mat =
				matrix(coef_df[coefname %chin% lag_vars][order(coefname)]$value, nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., cbind(diag(1, ar_lags - 1), matrix(rep(0, ar_lags - 1), ncol = 1)))
				}
			
			forecast_dates = tail(seq(tail(input_df$date, 1), tail(m$big_tstar_dates, 1), by = '1 month'), -1)
			
			purrr::accumulate(1:length(forecast_dates), function(accum, i)
				c_mat + {if (ar_lags == 0) 0 else a_mat %*% accum} + b_mat %*% f_mats[[i]],
				.init = y_0
				) %>%
				.[2:length(.)] %>%
				sapply(., function(x) matrix(x, ncol = 1)[[1, 1]]) %>%
				tibble(date = forecast_dates, varname = .varname, value = .)
			
			}) %>%
			bind_rows(.) %>%
			pivot_wider(., id_cols = 'date', names_from = varname, values_from = value) %>%
			arrange(., date)

		list(
			bdate = this_bdate,
			dfm_m_df = dfm_df
		)
	})
	
	for (x in results) {
		models[[as.character(x$bdate)]]$dfm_m_df <<- x$dfm_m_df
	}
})

## 2. Quarterly Variables (DFM-LASSO)  -----------------------------------------------------------------
#' Similar to monthly but with no constant
local({

	message('*** Forecasting Quarterly Variables')
	
	dfm_varnames = filter(variable_params, nc_method == 'dfm.q')$varname
	ar_lags = 1

	results = lapply(bdates, function(this_bdate) {
		
		message(str_glue('... Forecasting {this_bdate}'))
		
		m = models[[as.character(this_bdate)]]
		
		q_f_df =
			m$f_df %>%
			pivot_longer(., -date, names_to = 'varname') %>%
			mutate(., q = floor_date(date, 'quarters')) %>%
			select(., -date) %>%
			group_by(., varname, q) %>%
			summarize(., value = mean(value), .groups = 'drop') %>%
			pivot_wider(., names_from = varname, values_from = value) %>%
			rename(., date = q)
		
		# Only include variables which are available at that date
		stat_df =
			hist$wide$q$st[[as.character(this_bdate)]] %>%
			select(., all_of(c('date', dfm_varnames[dfm_varnames %in% colnames(.)]))) %>%
			# Add lags & inner join F
			{if (ar_lags > 0) bind_cols(., select(add_lagged_columns(., max_lag = ar_lags), -date)) else .} %>%
			inner_join(., q_f_df, by = 'date') %>%
			as.data.table(.)
		
		vars_to_forecast = colnames(stat_df) %>% keep(., ~ . %in% dfm_varnames)
		factor_vars = paste0('f', 1:m$big_r)
		
		dfm_results = lapply(dfm_varnames %>% set_names(., .), function(.varname) {
		
			lag_vars = {if (ar_lags == 0) NULL else paste0(.varname, '.l', 1:ar_lags)}
			
			# Get inputs - include all historical data
			input_df =
				stat_df[, c('date', .varname, factor_vars, lag_vars), with = F] %>%
				na.omit(.)
			
			y_mat = input_df[, c(.varname), with = F] %>% as.matrix(.)
			# Don't need to create constant here since glmnet will handle
			x_mat = input_df[, c(factor_vars, lag_vars), with = F] %>% as.matrix(.)
			
			glm_result = lapply(c(1), function(.alpha) {
				cv = glmnet::cv.glmnet(
					x = x_mat,
					y = y_mat,
					deviance = 'mae',
					alpha = .alpha,
					intercept = TRUE
					)
				tibble(alpha = .alpha, lambda = cv$lambda, mae = cv$cvm) %>%
					mutate(., min_lambda_for_given_alpha = (mae == min(mae))) %>%
					return(.)
				}) %>%
				bind_rows(.) %>%
				mutate(., min_overall = (mae == min(mae)))
				
			glm_optim = filter(glm_result, min_overall == TRUE)

			cv_plot =
				glm_result %>%
				ggplot(.) +
				geom_line(aes(x = log(lambda), y = mae, group = alpha, color = alpha)) +
				geom_point(
					data = glm_result %>% dplyr::filter(., min_lambda_for_given_alpha == TRUE),
					aes(x = log(lambda), y = mae), color = 'red'
				) +
				geom_point(
					data = glm_result %>% dplyr::filter(., min_overall == TRUE),
					aes(x = log(lambda), y = mae), color = 'green'
				) +
				labs(
					x = 'log(Lambda)', y = 'MAE', color = 'alpha',
					title = 'Elastic Net Hyperparameter Fit',
					subtitle = 'Red = MAE Minimizing Lambda for Given Alpha;
					Green = MAE Minimizing (Lambda, Alpha) Pair'
				)
				
				
			glm_obj = glmnet::glmnet(
					x = x_mat,
					y = y_mat,
					alpha = glm_optim$alpha,
					lambda = glm_optim$lambda
				)
				
			coef_mat = glm_obj %>% coef(.) %>% as.matrix(.)
				
			coef_df =
				coef_mat %>%
				as.data.frame(.) %>%
				rownames_to_column(., var = 'Covariate') %>%
				setNames(., c('coefname', 'value')) %>%
				as_tibble(.) %>%
				mutate(., coefname = ifelse(coefname == '(Intercept)', 'constant', coefname)) %>%
				as.data.table(.)
				
			# Standard OLS
			# 		coefDf =
			# 			lm(yMat ~ . - 1, xDf)$coef %>%
			#     		as.data.frame(.) %>% rownames_to_column(., 'coefname') %>% as_tibble(.) %>%
			# 			setNames(., c('coefname', 'value'))
			
			# Initialize lag Y_0 matrix
			y_0 =
				stat_df[order(-date)][date <= tail(input_df$date, 1), .varname, with = F] %>%
				head(., ar_lags) %>%
				as.matrix(., ncol = 1) 
			
			# Initialize factor matrix
			f_mats =
				as.data.table(q_f_df)[date > tail(input_df$date, 1), !'date'] %>%
				split(., 1:nrow(.)) %>%
				lapply(., function(x) t(x))
			
			# Initialize factor loadings matrix
			b_mat =
				coef_df[coefname %chin% factor_vars]$value %>%
				matrix(., nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., matrix(rep(0, (ar_lags - 1) * length(factor_vars)), ncol = length(factor_vars)))
				}
			
			# Initialize constant matrix
			c_mat = 
				c(coef_df[coefname == 'constant']$value, {if (ar_lags == 0) NULL else rep(0, ar_lags - 1)}) %>%
				matrix(., ncol = 1)
			
			# Lag weighted matrix
			a_mat =
				matrix(coef_df[coefname %chin% lag_vars][order(coefname)]$value, nrow = 1) %>%
				{
					if (ar_lags == 0) .
					else rbind(., cbind(diag(1, ar_lags - 1), matrix(rep(0, ar_lags - 1), ncol = 1)))
				}
			
			forecast_dates = tail(seq(tail(input_df$date, 1), tail(m$big_tstar_dates, 1), by = '3 months'), -1)
			
			forecast_df =
				purrr::accumulate(1:length(forecast_dates), function(accum, i)
					c_mat + {if (ar_lags == 0) 0 else a_mat %*% accum} + b_mat %*% f_mats[[i]],
					.init = y_0
				) %>%
				.[2:length(.)] %>%
				sapply(., function(x) matrix(x, ncol = 1)[[1, 1]]) %>%
				tibble(date = forecast_dates, varname = .varname, value = .)
			
			list(
				forecast_df = forecast_df,
				coef_df = coef_df,
				glm_optim = glm_optim,
				cv_plot = cv_plot
				)
			})
		
		dfm_df =
			dfm_results %>%
			map(., ~ .$forecast_df) %>%
			bind_rows(.) %>%
			pivot_wider(., id_cols = 'date', names_from = varname, values_from = value) %>%
			arrange(., date)
		
		glm_coef_list = map(dfm_results, ~ .$coef_df) 
		
		glm_optim_df = imap_dfr(dfm_results, function(x, i) bind_cols(varname = i, x$glm_optim))
		
		glm_cv_plots = map(dfm_results, ~ .$cv_plot)
		
		list(
			bdate = this_bdate,
			glm_coef_list = glm_coef_list,
			glm_optim_df = glm_optim_df,
			glm_cv_plots = glm_cv_plots,
			dfm_q_df = dfm_df
			)
		
		})
	
	for (x in results) {
		models[[as.character(x$bdate)]]$dfm_q_df <<- x$dfm_q_df
		models[[as.character(x$bdate)]]$glm_coef_list <<- x$glm_coef_list
		models[[as.character(x$bdate)]]$glm_optim_df <<- x$glm_optim_df
		models[[as.character(x$bdate)]]$glm_cv_plots <<- x$glm_cv_plots
	}
})

## 3. Detransform ---------------------------------------------------------------
local({
	
	message('*** Reversing Stationary Transformations')
	
	# Detransform monthly and quarterly forecasts
	results = lapply(bdates, function(this_bdate) {
		
		message(str_glue('... Detransforming {this_bdate}'))
		
		m = models[[as.character(this_bdate)]]
		
		this_m_hist = hist$wide$m$base[[as.character(this_bdate)]]
		this_q_hist = hist$wide$q$base[[as.character(this_bdate)]]
		
		m_df =
			m$dfm_m_df %>%
			as.data.table(.) %>%
			melt(., id.vars = 'date', value.name = 'value', variable.name = 'varname', na.rm = T) %>%
			merge(., variable_params[, c('varname', 'st')], by = 'varname', all.x = T) %>%
			split(., by = 'varname') %>%
			lapply(., function(x) 
				 x[order(date)] %>%
				 	.[, value := {
				 		if (.$st[[1]] == 'none') NA
				 		else if (.$st[[1]] == 'base') value
				 		else if (.$st[[1]] == 'dlog')
				 			undlog(value, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else if (.$st[[1]] == 'diff1')
				 			undiff(value, 1, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else if (.$st[[1]] == 'pchg')
				 			unpchg(value, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else if (.$st[[1]] == 'apchg')
				 			unapchg(value, 12, tail(filter(this_m_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
				 		else stop('Error')
				 		}]
				) %>%
			rbindlist(.) %>%
			.[, st := NULL] %>%
			dcast(., date ~ varname, value.var = 'value', fill = NA) %>%
			as_tibble(.)
		
		q_df =
			m$dfm_q_df %>%
			as.data.table(.) %>%
			melt(., id.vars = 'date', value.name = 'value', variable.name = 'varname', na.rm = T) %>%
			merge(., variable_params[, c('varname', 'st')], by = 'varname', all.x = T) %>%
			split(., by = 'varname') %>%
			lapply(., function(x) 
				x[order(date)] %>%
					.[, value := {
						if (.$st[[1]] == 'none') NA
						else if (.$st[[1]] == 'base') value
						else if (.$st[[1]] == 'dlog')
							undlog(value, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else if (.$st[[1]] == 'diff1')
							undiff(value, 1, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else if (.$st[[1]] == 'pchg')
							unpchg(value, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else if (.$st[[1]] == 'apchg')
							unapchg(value, 4, tail(filter(this_q_hist, date < x$date[[1]]), 1)[[.$varname[[1]]]])
						else stop('Error')
					}]
				) %>%
			rbindlist(.) %>%
			.[, st := NULL] %>%
			dcast(., date ~ varname, value.var = 'value', fill = NA) %>%
			as_tibble(.)
		
		list(
			bdate = this_bdate,
			m_ut = m_df,
			q_ut = q_df
			)
		})
	
	for (x in results) {
		models[[as.character(x$bdate)]]$pred_m_ut0 <<- x$m_ut
		models[[as.character(x$bdate)]]$pred_q_ut0 <<- x$q_ut
	}
})


## 4. Calculate GDP Nowcast  -----------------------------------------------------------------
local({
	
	message('*** Adding Calculated Variables')
	
	results = lapply(bdates, function(this_bdate) {
		
		message(str_glue('... Forecasting {this_bdate}'))
		m = models[[as.character(this_bdate)]]
		
		pred_q_ut1 =
			m$pred_q_ut0 %>%
			transmute(
				.,
				date,
				govt = govtf + govts,
				ex = exg + exs,
				im = img + ims,
				nx = ex - im,
				pdin = pdinstruct + pdinequip + pdinip,
				pdi = pdin + pdir + pceschange,
				pces = pceshousing + pceshealth + pcestransport + pcesrec + pcesfood + pcesfinal + pcesother + pcesnonprofit,
				pcegn = pcegnfood + pcegnclothing + pcegngas + pcegnother,
				pcegd = pcegdmotor + pcegdfurnish + pcegdrec + pcegdother,
				pceg = pcegn + pcegd,
				pce = pceg + pces,
				gdp = pce + pdi + nx + govt
			)
		
		pred_m_ut1 =
			m$pred_m_ut0 %>%
			transmute(
				.,
				date,
				psr = ps/pid
			)
		
		list(
			bdate = this_bdate,
			pred_q_ut1 = pred_q_ut1,
			pred_m_ut1 = pred_m_ut1
			)
	})
	
	for (x in results) {
		models[[as.character(x$bdate)]]$pred_m_ut1 <<- x$pred_m_ut1
		models[[as.character(x$bdate)]]$pred_q_ut1 <<- x$pred_q_ut1
	}
})


## 5. Aggregate & Create Display Formats  -----------------------------------------------------------------
local({
	
	results = lapply(bdates, function(this_bdate) {
		
		message(str_glue('... Aggregating {this_bdate}'))
		m = models[[as.character(this_bdate)]]
		
		pred_ut = lapply(c('m', 'q') %>% set_names(., .), function(freq)
			{if (freq == 'm') list(m$pred_m_ut0, m$pred_m_ut1) else list(m$pred_q_ut0, m$pred_q_ut1)} %>%
				reduce(., function(x, y) full_join(x, y, by = 'date')) %>%
				arrange(., date)
			)
		
		this_hist = lapply(c('m', 'q') %>% set_names(., .), function(freq)
			hist$wide[[freq]]$base[[as.character(this_bdate)]] %>%
				as.data.table(.) %>%
				melt(., id.vars = 'date', variable.name = 'varname', na.rm = T)
			)
		
		pred_flat = 
			# Map each frequency x display form combination
			expand_grid(form = c('d1', 'd2'), freq = c('m', 'q')) %>%
			purrr::transpose(.) %>%
			lapply(., function(z) {
				
				df =
					pred_ut[[z$freq]] %>%
						as.data.table(.) %>%
						melt(., id.vars = 'date', value.name = 'value', variable.name = 'varname', na.rm = T) %>%
						merge(
							.,
							rename(variable_params[, c('varname', z$form)], 'transform' = z$form),
							by = 'varname', all.x = T
							) %>%
						split(., by = 'varname') %>%
						lapply(., function(x) {
							# message(x$varname[[1]])
							x[order(date)] %>%
								# Bind historical date
								rbind(
									this_hist[[z$freq]][date < .$date[[1]] & varname == .$varname[[1]]] %>%
										.[, transform := x$transform[[1]]] %>%
										last(.),
										.
									) %>%
								.[, value := {
									if (.$transform[[1]] == 'none') NA
									else if (.$transform[[1]] == 'base') value
									else if (.$transform[[1]] == 'dlog') dlog(value)
									else if (.$transform[[1]] == 'diff1') diff1(value)
									else if (.$transform[[1]] == 'pchg') pchg(value)
									else if (.$transform[[1]] == 'apchg') apchg(value, 12)
									else stop('Error')
								}] %>%
								tail(., -1)
						}) %>%
						rbindlist(.) %>%
						.[, transform := NULL] %>%
						na.omit(.)
				
				list(
					df = df,
					form = z$form,
					freq = z$freq
					)
			}) %>%
			map_dfr(., function(z) z$df[, c('form', 'freq') := list(z$form, z$freq)])
			
		list(
			bdate = this_bdate,
			pred_flat = pred_flat
			)
		})
	
	pred_flat =
		map_dfr(results, function(z) as_tibble(mutate(z$pred_flat, bdate = z$bdate))) %>%
		# Varname gets casted as a factor
		mutate(., varname = as.character(varname))
	
	pred_wide =
		lapply(split(as.data.table(pred_flat), by = 'bdate', keep.by = F), function(x)
			lapply(split(x, by = 'freq', keep.by = F), function(y)
				lapply(split(y, by = 'form', keep.by = F), function(z)
					dcast(z, date ~ varname, value.var = 'value')[order(date)] %>%
						as_tibble(.)
					)
				)
			)
	
	print(
		pred_wide$`2022-02-04`$q$d1[, c('date', 'gdp', 'pce', 'pdi', 'ex', 'im', 'govt')]
		)
	
})

