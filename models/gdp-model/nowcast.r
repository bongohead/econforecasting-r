#'  Run this script on scheduler after close of business each day

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(DIR, 'logs', 'pull-data.log'), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(paste0('Run ', Sys.Date()))
}

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)
library(lubridate)

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
ext = list()

## Load Variable Defs ----------------------------------------------------------'
variable_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'variables')
release_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'data-releases')

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message('***** Getting Releases History')

	fred_releases =
		variable_params %>%
		group_by(., relkey) %>%
		summarize(., n_varnames = n(), varnames = jsonlite::toJSON(fullname), .groups = 'drop') %>%
		left_join(
			.,
			variable_params  %>%
				filter(., nc_dfm_input == 1) %>%
				group_by(., relkey) %>%
				summarize(., n_dfm_varnames = n(), dfm_varnames = jsonlite::toJSON(fullname), .groups = 'drop'),
			variable_params %>%
				group_by(., relkey) %>%
				summarize(., n_varnames = n(), varnames = jsonlite::toJSON(fullname), .groups = 'drop'),
			by = 'relkey'
		) %>%
		left_join(., release_params, by = 'relkey') %>%
		# Now create a column of included releaseDates
		left_join(
			.,
			purrr::map_dfr(purrr::transpose(filter(., relsc == 'fred')), function(x) {

				httr::RETRY(
					'GET',
					str_glue(
						'https://api.stlouisfed.org/fred/release/dates?',
						'release_id={x$relsckey}&realtime_start=2020-01-01',
						'&include_release_dates_with_no_data=true&api_key={CONST$FRED_API_KEY}&file_type=json'
					),
					times = 10
					) %>%
					httr::content(., as = 'parsed') %>%
					.$release_dates %>%
					sapply(., function(y) y$date) %>%
					tibble(relkey = x$relkey, reldates = .)
				}) %>%
				group_by(., relkey) %>%
				summarize(., reldates = jsonlite::toJSON(reldates), .groups = 'drop'),
			by = 'relkey'
			)

	releases$fred <<- fred_releases
})

## 2. Combine ----------------------------------------------------------
local({

	releases_final <<- bind_rows(releases)
})

# Historical Data ----------------------------------------------------------

## 1. FRED ----------------------------------------------------------
local({

	message('***** Importing FRED Data')

	fred_data =
		variable_params %>%
		purrr::transpose(.)	%>%
		purrr::keep(., ~ .$source == 'fred') %>%
		purrr::imap_dfr(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_data(
					x$sckey,
					CONST$FRED_API_KEY,
					.freq = x$freq,
					.return_vintages = TRUE,
					.verbose = F
					) %>%
				transmute(
					.,
					sourcename = 'fred',
					varname = x$varname,
					transform = 'base',
					freq = x$freq,
					date,
					vdate = vintage_date,
					value
					) %>%
				filter(
					.,
					date >= as_date('2010-01-01'),
					vdate >= as_date('2010-01-01')
					)
			})

	hist$fred <<- fred_data
})

## 2. Yahoo Finance ----------------------------------------------------------
local({

	message('***** Importing Yahoo Finance Data')

	yahoo_data =
		variable_params %>%
		purrr::transpose(.) %>%
		purrr::keep(., ~ .$source == 'yahoo') %>%
		purrr::map_dfr(., function(x) {
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
					sourcename = 'yahoo',
					varname = x$varname,
					transform = 'base',
					freq = x$freq,
					date,
					vdate = date,
					value = as.numeric(value)
					) %>%
				return(.)
		})

	hist$yahoo <<- yahoo_data
})

## 3. Calculated Variables ----------------------------------------------------------
local({

	message('***** Adding Calculated Variables')

	fred_data =
		variable_params %>%
		filter(., str_detect(fullname, 'Treasury Yield') | varname == 'ffr') %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(x) {
			get_fred_data(x$sckey, CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
				transmute(., varname = x$varname, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date('2010-01-01'))
		})

	# Monthly aggregation & append EOM with current val
	fred_data_cat =
		fred_data %>%
		group_split(., varname) %>%
		# Add monthly values for current month
		map_dfr(., function(x)
			x %>%
				mutate(., date = as.Date(paste0(year(date), '-', month(date), '-01'))) %>%
				group_by(., varname, date) %>%
				summarize(., value = mean(value), .groups = 'drop') %>%
				mutate(., freq = 'm')
		)

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		variable_params %>%
		purrr::transpose(.) %>%
		map_chr(., ~.$varname) %>%
		unique(.) %>%
		purrr::keep(., ~ str_sub(., 1, 1) == 't' & str_length(.) == 4) %>%
		tibble(varname = .) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))

	# Create training dataset from SPREAD from ffr - fitted on last 3 months
	train_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		select(., -freq) %>%
		filter(., date >= add_with_rollback(Sys.Date(), months(-3))) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(., transmute(filter(fred_data_cat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	#' Calculate DNS fit
	#'
	#' @param df: (tibble) A tibble continuing columns obsDate, value, and ttm
	#' @param return_all: (boolean) FALSE by default.
	#' If FALSE, will return only the MAPE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			mutate(
				.,
				f1 = 1,
				f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm),
				f3 = f2 - exp(-1 * lambda * ttm)
			) %>%
			group_by(date) %>%
			group_split(.) %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				bind_cols(x, fitted = fitted(reg)) %>%
					mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
					mutate(., resid = value - fitted)
			}) %>%
			bind_rows(.) %>%
			{
				if (return_all == FALSE) summarise(., mse = mean(abs(resid))) %>% .$mse
				else .
			} %>%
			return(.)
	}

	# Find MSE-minimizing lambda value
	optim_lambda =
		optimize(
			get_dns_fit,
			df = train_df,
			return_all = FALSE,
			interval = c(-1, 1),
			maximum = FALSE
		)$minimum

	hist_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		select(., -freq) %>%
		filter(., date >= add_with_rollback(Sys.Date(), months(-120))) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(., transmute(filter(fred_data_cat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	# Store DNS coefficients
	dns_data =
		get_dns_fit(df = hist_df, optim_lambda, return_all = TRUE) %>%
		group_by(., date) %>%
		summarize(., tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3)) %>%
		pivot_longer(., -date, names_to = 'varname', values_to = 'value') %>%
		transmute(
			.,
			sourcename = 'calc',
			varname,
			transform = 'base',
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
		hist$fred %>%
		filter(., varname == 'cpi' & transform == 'base' & freq == 'm') %>%
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
			sourcename = 'calc',
			varname = 'inf',
			transform = 'base',
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
		purrr::map_dfr(., function(x) {

			# Use weekly data as mortgage pulls are weekly
			var_w_df =
				filter(hist$fred, varname == x$var_w & transform == 'base' & freq == 'w') %>%
				select(., varname, date, vdate, value)

			var_d_df =
				filter(hist$fred, varname == x$var_d & transform == 'base' & freq == 'd') %>%
				select(., varname, date, vdate, value)

			# Get week-end days
			w_obs_dates =
				filter(hist$fred, varname == x$var_w & transform == 'base' & freq == 'w') %>%
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
						sourcename = 'calc',
						varname = x$varname,
						transform = 'base',
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

	hist$calc <<- hist_calc
})


## 4. Aggregate Frequencies ----------------------------------------------------------
local({

	hist_agg_0 = bind_rows(hist)

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
		group_by(., sourcename, varname, transform, freq, date, vdate) %>%
		summarize(., value = mean(value), .groups = 'drop')

	quarterly_agg =
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w', 'm')) %>%
		mutate(
			.,
			freq = 'q',
			date = lubridate::floor_date(date, 'quarter')
			) %>%
		group_by(., sourcename, varname, transform, freq, date, vdate) %>%
		summarize(., value = mean(value), count = n(), .groups = 'drop') %>%
		filter(., count == 3) %>%
		select(., -count)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q'))

	hist_agg <<- hist_agg
})

# Select  ----------------------------------------------------------

## Create List of Vintage Dates ----------------------------------------------------------
this_vdate = Sys.Date()

## Add Stationary Transformations (Last Vintage Date) ----------------------------------------------------------
local({
	
	transformed_data =
		hist_agg %>%
		group_split(., sourcename, varname, transform, freq) %>%
		map_dfr(., function(df_by_var_freq) {
			
			# Get last available obs for each dates as pf this vintage date
			last_obs =
				filter(df_by_var_freq, vdate <= this_vdate) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				select(., -transform)
			
			# Get variable params
			variable_param = purrr::transpose(filter(variable_params, varname == df_by_var_freq$varname[[1]]))[[1]]
			if (is.null(variable_param)) stop(str_glue('Missing {df_by_var_freq$varname[[1]]} in variable_params'))
			
			# Now apply transformations
			last_obs_transformed = lapply(c('st', 'd1', 'd2'), function(.form) {
				transform = variable_param[[.form]]
				if (transform == 'none') return(NULL)
				last_obs %>%
					mutate(
						.,
						value = {
							if (transform == 'base') value
							else if (transform == 'dlog') dlog(value)
							else if (transform == 'diff1') diff1(value)
							else if (transform == 'pchg') pchg(value)
							else if (transform == 'apchg') apchg(value, {if (variable_param$freq == 'q') 4 else 12})
							else stop('Error')
							},
						form = .form
						) %>%
					na.omit(.)
				}) %>%
				purrr::keep(., ~ !is.null(.) & is_tibble(.)) %>%
				bind_rows(., mutate(last_obs, form = 'base'))
				
				return(last_obs_transformed)
			})

	hist_all <<- transformed_data
})




