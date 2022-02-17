#'  Run this script on scheduler after close of business each day
#'  Assumes rate data is not subject to revisions

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-pull-inputs'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = F
IMPORT_DATE_START = '2008-01-01'

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(readxl)
library(httr)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)
library(glmnet)
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
releases = list()
hist = list()

## Load Variable Defs ----------------------------------------------------------
variable_params = read_excel(
	file.path(EF_DIR, 'modules', 'nowcast-model', 'nowcast-model-inputs.xlsx'),
	sheet = 'variables'
)
release_params = read_excel(
	file.path(EF_DIR, 'modules', 'nowcast-model', 'nowcast-model-inputs.xlsx'),
	sheet = 'releases'
)

## Set Backtest Dates  ----------------------------------------------------------
local({
	# Include all days in last 3 months plus one random day per month before that
	contiguous = seq(today() - days(150), today(), by = '1 day')

	old =
		seq(
			as_date('2020-01-01'),
			add_with_rollback(floor_date(min(contiguous), 'months'), months(-1)),
			by = '1 month'
		) %>%
		map(., ~ sample(seq(floor_date(., 'month'), ceiling_date(., 'month') - days(1), '1 day'), 1)) %>%
		unlist(.) %>%
		as_date(.)

	bdates = c(old, contiguous)

	bdates <<- bdates
})

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message(str_glue('*** Getting Releases History | {format(now(), "%H:%M")}'))

	fred_releases =
		variable_params %>%
		group_by(., relkey) %>%
		summarize(., n_varnames = n(), varnames = toJSON(fullname), .groups = 'drop') %>%
		left_join(
			.,
			variable_params  %>%
				filter(., nc_dfm_input == T) %>%
				group_by(., relkey) %>%
				summarize(., n_dfm_varnames = n(), dfm_varnames = toJSON(fullname), .groups = 'drop'),
			variable_params %>%
				filter(., nc_dfm_input == T) %>%
				group_by(., relkey) %>%
				summarize(., n_varnames = n(), varnames = toJSON(fullname), .groups = 'drop'),
			by = 'relkey'
		) %>%
		left_join(., release_params, by = 'relkey') %>%
		# Now create a column of included release dates
		left_join(
			.,
			map_dfr(purrr::transpose(filter(., relsc == 'fred')), function(x) {
				RETRY(
					'GET',
					str_glue(
						'https://api.stlouisfed.org/fred/release/dates?',
						'release_id={x$relsckey}&realtime_start={min(bdates)}',
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
				summarize(., reldates = toJSON(reldates), .groups = 'drop'),
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
			message(str_glue('**** Pull {i}: {x$varname}'))
			res =
				get_fred_data(
					x$sckey,
					CONST$FRED_API_KEY,
					.freq = x$freq,
					.return_vintages = T,
					.obs_start = IMPORT_DATE_START,
				) %>%
				transmute(., varname = x$varname, freq = x$freq, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date(IMPORT_DATE_START), vdate >= as_date(IMPORT_DATE_START))
			message(str_glue('**** Count: {nrow(res)}'))
			return(res)
		})

	hist$raw$fred <<- fred_data
})

## 2. Yahoo Finance ----------------------------------------------------------
local({

	message(str_glue('*** Importing Yahoo Finance Data | {format(now(), "%H:%M")}'))

	yahoo_data =
		variable_params %>%
		purrr::transpose(.) %>%
		keep(., ~ .$source == 'yahoo') %>%
		map_dfr(., function(x) {
			url =
				paste0(
					'https://query1.finance.yahoo.com/v7/finance/download/', x$sckey,
					'?period1=', as.numeric(as.POSIXct(as_date(IMPORT_DATE_START))),
					'&period2=', as.numeric(as.POSIXct(Sys.Date() + days(1))),
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

	# For each vintage date, pull all CPI values from the latest available vintage for each date
	# This intelligently handles revisions and prevents
	# duplications of CPI values by vintage dates/obs date
	cpi_df =
		hist$raw$fred %>%
		filter(., varname == 'cpi0' & freq == 'm') %>%
		select(., date, vdate, value)

	cpi_data =
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
			varname = 'cpi',
			freq = 'm',
			date,
			vdate,
			value
		)

	pcepi_df =
		hist$raw$fred %>%
		filter(., varname == 'pcepi0' & freq == 'm') %>%
		select(., date, vdate, value)

	pcepi_data =
		pcepi_df %>%
		group_split(., vdate) %>%
		purrr::map_dfr(., function(x)
			filter(pcepi_df, vdate <= x$vdate[[1]]) %>%
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
			varname = 'pcepi',
			freq = 'm',
			date,
			vdate,
			value
		)


	hist_calc = bind_rows(cpi_data, pcepi_data)

	hist$raw$calc <<- hist_calc
})

## 4. Aggregate Frequencies ----------------------------------------------------------
local({

	message(str_glue('*** Aggregating Monthly & Quarterly Data | {format(now(), "%H:%M")}'))

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

	message(str_glue('*** Splitting By Vintage Date | {format(now(), "%H:%M")}'))

	# Check min dates
	# The latest min date for PCA inputs should be no earlier than the last PCA input variable
	message('***** Variables Dates:')
	hist$agg %>%
		group_by(., varname) %>%
		summarize(., min_dt = min(date)) %>%
		arrange(., desc(min_dt)) %>%
		print(., n = 100)

	last_obs_by_vdate =
		hist$agg %>%
		as.data.table(.) %>%
		split(., by = c('varname', 'freq')) %>%
		lapply(., function(x)  {

			# message(str_glue('**** Getting last vintage dates for {x$varname[[1]]}'))
			# Get last observation for every vintage date
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
				) %>%
				.[, date := as_date(date)]

			# For each bdate, get last vdate before this value
			lapply(bdates, function(this_bdate)
				last_obs_for_all_vdates %>%
					.[vdate <= this_bdate] %>%
					{if (nrow(.) == 0) NULL else .[vdate == max(vdate)] %>% .[, bdate := this_bdate]}
			) %>%
				rbindlist(.)
		}) %>%
		rbindlist(.)

	hist$base <<- last_obs_by_vdate
})


## 6. Add Stationary Transformations ----------------------------------------------------------
local({

	message(str_glue('*** Adding Stationary Transforms | {format(now(), "%H:%M")}'))

	stat_groups =
		hist$base %>%
		split(., by = c('varname', 'freq', 'bdate')) %>%
		unname(.)

	stat_final =
		stat_groups %>%
		imap(., function(x, i) {

			if (i %% 1000 == 0) message(str_glue('**** Transforming {i} of {length(stat_groups)}'))

			variable_param = purrr::transpose(filter(variable_params, varname == x$varname[[1]]))[[1]]
			if (is.null(variable_param)) stop(str_glue('Missing {x$varname[[1]]} in variable_params'))

			last_obs_by_vdate_t = lapply(c('st', 'd1', 'd2'), function(this_form) {
				transform = variable_param[[this_form]]
				copy(x) %>%
					.[,
					  value := {
					  	if (transform == 'none') NA
					  	else if (transform == 'base') value
					  	else if (transform == 'log') log(value)
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

	message(str_glue('*** Creating Wide Matrices | {format(now(), "%H:%M")}'))

	wide =
		hist$flat %>%
		split(., by = 'freq', keep.by = F) %>%
		lapply(., function(x)
			split(x, by = 'form', keep.by = F) %>%
				lapply(., function(y)
					split(y, by = 'bdate', keep.by = T) %>%
						lapply(., function(z) {
							# message(z$bdate[[1]])
							dcast(select(z, -bdate), date ~ varname, value.var = 'value') %>%
								.[, date := as_date(date)] %>%
								.[order(date)] %>%
								as_tibble(.)
						})
				)
		)

	wide_last <<- lapply(wide, function(x) lapply(x, function(y) tail(y, 1)[[1]]))

	hist$wide <<- wide
	hist$wide_last <<- wide_last
})
