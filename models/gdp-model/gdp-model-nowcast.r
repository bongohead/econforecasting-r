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
test_vdates = c(
	# Include all days in last 6 months
	seq(today() - days(180), today(), by = '1 day'),
	# Plus one random day per month before that
	seq(as_date('2012-01-01'), add_with_rollback(today() - days(180), months(-1)), by = '1 month') %>%
		map(., ~ sample(seq(floor_date(., 'month'), ceiling_date(., 'month'), '1 day'), 1))
	) %>% sort(.)
	
	
# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message('***** Getting Releases History')

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

	message('***** Importing FRED Data')

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

	message('***** Importing Yahoo Finance Data')

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

	message('***** Adding Calculated Variables')

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

	hist_agg_0 = bind_rows(hist$raw)

	monthly_agg =
		# Get all daily/weekly varnames with pre-existing data
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w')) %>%
		# Add monthly values for current month
		mutate(
			.,
			freq = 'm',
			date = lubridate::floor_date(date, 'month')
			) %>%
		group_by(., varname, freq, date, vdate) %>%
		summarize(., value = mean(value), .groups = 'drop')

	quarterly_agg =
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w', 'm')) %>%
		mutate(
			.,
			freq = 'q',
			date = lubridate::floor_date(date, 'quarter')
			) %>%
		group_by(., varname, freq, date, vdate) %>%
		summarize(., value = mean(value), count = n(), .groups = 'drop') %>%
		filter(., count == 3) %>%
		select(., -count)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q'))

	hist$agg <<- hist_agg
})

## 5. Add Stationary Transformations ----------------------------------------------------------
local({
	
	message('*** Adding Stationary Transformations')
	# Get last observation date available for each possible vintage	
	last_obs_by_vdate =
		hist$agg %>%
		as.data.table(.) %>% 
		split(., by = c('varname', 'freq')) %>%
		lapply(., function(x) 
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
			) %>%
		rbindlist(.)
	
	stat_groups =
		last_obs_by_vdate %>%
		.[vdate >= as_date('2020-01-01')] %>%
		split(., by = c('varname', 'freq', 'vdate')) %>%
		unname(.)
		
	stat_final =
		stat_groups %>%
		imap(., function(x, i) {
			
			if (i %% 1000 == 0) message(str_glue('... Transforming {i} of {length(stat_groups)}'))
			
			variable_param = purrr::transpose(filter(variable_params, varname == x$varname[[1]]))[[1]]
			if (is.null(variable_param)) stop(str_glue('Missing {x$varname[[1]]} in variable_params'))
			
			last_obs_by_vdate_t = lapply(c('st', 'd1', 'd2'), function(form) {
				transform = variable_param[[form]]
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
					.[, form := form]
				}) %>%
				rbindlist(.)
		}) %>%
		rbindlist(.) %>%
		na.omit(.)

	hist_all <<- transformed_data
})

## Add Stationary Transformations (Last Vintage Date) ----------------------------------------------------------

## Create Monthly/Quarterly Matrices
local({

	hist_wide =
		hist_all %>%
		data.table::as.data.table(.) %>%
		split(., by = 'freq') %>%
		lapply(., function(x)
			split(x, by = 'form') %>%
				lapply(., function(y)
					as_tibble(y) %>%
						dplyr::select(., -freq, -form) %>%
						tidyr::pivot_wider(., id_cols = c('date'), names_from = varname, values_from = value) %>%
						dplyr::arrange(., date)
				)
		)

	hist_wide <<- hist_wide
})



# Nowcast -----------------------------------------------------------------

local({

	quartersForward = 1

	pcaVarnames = ef$paramsDf %>% dplyr::filter(., dfminput == TRUE) %>% .$varname

	pcaVariablesDf =
		ef$h$m$st %>%
		dplyr::select(., date, all_of(pcaVarnames)) %>%
		dplyr::filter(., date >= as.Date('2010-01-01'))

	bigTDates = pcaVariablesDf %>% dplyr::filter(., !if_any(everything(), is.na)) %>% .$date
	bigTauDates = pcaVariablesDf %>% dplyr::filter(., if_any(everything(), is.na)) %>% .$date
	bigTStarDates =
		bigTauDates %>%
		tail(., 1) %>%
		seq(
			from = .,
			to =
				# Get this quarter
				econforecasting::strdateToDate(paste0(lubridate::year(.), 'Q', lubridate::quarter(.))) %>%
				# Get next quarter minus a month
				lubridate::add_with_rollback(., months(3 * (1 + quartersForward) - 1)),
			by = '1 month'
		) %>%
		.[2:length(.)]


	bigTDate = tail(bigTDates, 1)
	bigTauDate = tail(bigTauDates, 1)
	bigTStarDate = tail(bigTauDates, 1)

	bigT = length(bigTDates)
	bigTau = bigT + length(bigTauDates)
	bigTStar = bigTau + length(bigTStarDates)

	timeDf =
		tibble(
			date = as.Date(c(pcaVariablesDf$date[[1]], bigTDate, bigTauDate, bigTStarDate)),
			time = c('1', 'T', 'Tau', 'T*')
		)



	ef$nc$pcaVariablesDf <<- pcaVariablesDf
	ef$nc$pcaVarnames <<- pcaVarnames
	ef$nc$quartersForward <<- quartersForward
	ef$nc$bigTDates <<- bigTDates
	ef$nc$bigTauDates <<- bigTauDates
	ef$nc$bigTStarDates <<- bigTStarDates
	ef$nc$bigTDate <<- bigTDate
	ef$nc$bigTauDate <<- bigTauDate
	ef$nc$bigTStarDate <<- bigTStarDate
	ef$nc$bigT <<- bigT
	ef$nc$bigTau <<- bigTau
	ef$nc$bigTStar <<- bigTStar
	ef$nc$timeDf <<- timeDf
})




