#'  Run this script on scheduler after close of business each day

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'GDP-MODEL-NOWCAST'
DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

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

## Load Variable Defs ----------------------------------------------------------
variable_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'gdp-model-inputs')
release_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'data-releases')

## Set Backtest Dates  ----------------------------------------------------------
bdates = c(
	# Include all days in last 4 months
	seq(today() - days(120), today(), by = '1 day'),
	# Plus one random day per month before that
	seq(as_date('2016-04-01'), add_with_rollback(today() - days(180), months(-1)), by = '1 month') %>%
		map(., ~ sample(seq(floor_date(., 'month'), ceiling_date(., 'month'), '1 day'), 1))
	) %>% sort(.)


# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message('*** Getting Releases History')

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

# Nowcast -----------------------------------------------------------------

## 1. Dates -----------------------------------------------------------------
local({

	quarters_forward = 2
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
		model[[as.character(x$bdate)]] <<- x
	}
	model$quarters_forward <<- quarters_forward
	model$pca_varnames <<- pca_varnames
})


## Extract PCA Factors -----------------------------------------------------------------
local({

	xDf = ef$nc$pcaVariablesDf %>% dplyr::filter(., date %in% ef$nc$bigTDates)

	xMat =
		xDf %>%
		dplyr::select(., -date) %>%
		as.matrix(.) %>%
		scale(.)

	lambdaHat = eigen(t(xMat) %*% xMat) %>% .$vectors
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
		labs(title = 'Percent of Variance Explained', x = 'Factors (R)', y = 'Cumulative % of Total Variance Explained', fill = NULL) +
		ggthemes::theme_fivethirtyeight()


	bigR =
		screeDf %>%
		dplyr::filter(., ic1 == min(ic1)) %>%
		.$factors + 2
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


	ef$nc$factorWeightsDf <<- factorWeightsDf
	ef$nc$screeDf <<- screeDf
	ef$nc$screePlot <<- screePlot
	ef$nc$bigR <<- bigR
	ef$nc$pcaInputDf <<- xDf
	ef$nc$zDf <<- zDf
	ef$nc$zPlots <<- zPlots
})




