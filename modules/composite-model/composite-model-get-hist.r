#' This gets historical data for website

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-get-hist'
EF_DIR = Sys.getenv('EF_DIR')
IMPORT_DATE_START = '2016-01-01' #'2007-01-01' Reduced 7/6/22 due to vintage date limit on ALFRED

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
variable_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_variables'))
release_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_hist_releases'))

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message(str_glue('*** Getting Releases History | {format(now(), "%H:%M")}'))

	fred_releases =
		release_params %>%
		filter(., source == 'fred') %>%
		purrr::transpose(.) %>%
		map_dfr(., function(x)
			RETRY(
				'GET',
				str_glue(
					'https://api.stlouisfed.org/fred/release/dates?',
					'release_id={x$source_key}&realtime_start=2010-01-01',
					'&include_release_dates_with_no_data=true&api_key={CONST$FRED_API_KEY}&file_type=json'
				),
				times = 10
			) %>%
				content(., as = 'parsed') %>%
				.$release_dates %>%
				{tibble(
					release = x$id,
					date = sapply(., function(y) y$date)
				)}
		)

	releases$raw$fred <<- fred_releases
})

## 2. Combine ----------------------------------------------------------
local({
	releases$final <<- bind_rows(releases$raw)
})

## 3. SQL ----------------------------------------------------------
local({

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_hist_release_dates')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		releases$final %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'forecast_hist_release_dates',
				'ON CONFLICT (release, date) DO NOTHING'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_hist_release_dates')$count)
	message('***** Rows Added: ', final_count - initial_count)
})

# Historical Data ----------------------------------------------------------

## 1. FRED ----------------------------------------------------------
local({

	message('*** Importing FRED Data')

	fred_data =
		variable_params %>%
		purrr::transpose(.) %>%
		keep(., ~ .$hist_source == 'fred') %>%
		imap_dfr(., function(x, i) {
			message(str_glue('**** Pull {i}: {x$varname}'))
			res =
				get_fred_data(
					x$hist_source_key,
					CONST$FRED_API_KEY,
					.freq = x$hist_source_freq,
					.return_vintages = T,
					.obs_start = IMPORT_DATE_START,
					.verbose = T
				) %>%
				transmute(., varname = x$varname, freq = x$hist_source_freq, date, vdate = vintage_date, value) %>%
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
		keep(., ~ .$hist_source == 'yahoo') %>%
		map_dfr(., function(x) {
			url =
				paste0(
					'https://query1.finance.yahoo.com/v7/finance/download/', x$hist_source_key,
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
					freq = x$hist_source_freq,
					date,
					vdate = date,
					value = as.numeric(value)
				) %>%
				return(.)
		})

	hist$raw$yahoo <<- yahoo_data
})

## 3. BLOOM  ----------------------------------------------------------
local({

	bloom_data =
		variable_params %>%
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
					varname = x$varname,
					freq = 'd',
					date = as_date(dateTime),
					vdate = date,
					value
				) %>%
				na.omit(.)
			
			# Add sleep due to bot detection
			Sys.sleep(runif(1, 5, 10))
			
			return(res)
		})

	hist$raw$bloom <<- bloom_data
})

## 4. AFX  ----------------------------------------------------------
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
			select(filter(variable_params, hist_source == 'afx'), varname, hist_source_key),
			by = c('varname_scrape' = 'hist_source_key')
		) %>%
		distinct(.) %>%
		transmute(., varname, freq = 'd', date, vdate = date, value)

	hist$raw$afx <<- afx_data
})

## 5. ECB ---------------------------------------------------------------------
local({
	
	# Migrated to ECBESTRVOLWGTTRMDMNRT!
	# Note 2022-07-22: deprecated!
	# https://sdw.ecb.europa.eu/quickview.do?SERIES_KEY=438.EST.B.EU000A2QQF16.CR
	estr_data = read_csv(
		'https://sdw.ecb.europa.eu/quickviewexport.do?SERIES_KEY=438.EST.B.EU000A2QQF16.CR&type=csv',
		skip = 6,
		col_names = c('date', 'value', 'obs', 'calc1', 'calc2'),
		col_types = 'DdcDD'
		) %>%
		transmute(., varname = 'estr', freq = 'd', date, vdate = date, value)

	hist$raw$ecb <<- estr_data
})

## 6. BOE ---------------------------------------------------------------------
local({
	
	# Bank rate
	boe_keys = tribble(
		~ varname, ~ url,
		'ukbankrate', 'https://www.bankofengland.co.uk/boeapps/database/Bank-Rate.asp'
	)
	
	boe_data = map_dfr(purrr::transpose(boe_keys), function(x)
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
		transmute(
			.,
			varname = 'ukbankrate',
			freq = 'd',
			date,
			vdate = date,
			value
		)

	hist$raw$boe <<- boe_data
})

## 7. Calculated Variables ----------------------------------------------------------
local({

	message('*** Adding Calculated Variables')

	# For each vintage date, pull all CPI values from the latest available vintage for each date
	# This intelligently handles revisions and prevents
	# duplications of CPI values by vintage dates/obs date
	cpi_df =
		hist$raw$fred %>%
		filter(., varname == 'cpi' & freq == 'm') %>%
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
		filter(., varname == 'pcepi' & freq == 'm') %>%
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

## 8. Verify ----------------------------------------------------------
local({

	missing_varnames = variable_params$varname %>% .[!. %in% unique(bind_rows(hist$raw)$varname)]

	message('*** Missing Variables: ')
	cat(paste0(missing_varnames, collapse = '\n'))
})


## 9. Aggregate Frequencies ----------------------------------------------------------
local({

	message(str_glue('*** Aggregating Monthly & Quarterly Data | {format(now(), "%H:%M")}'))

	hist_agg_0 =
		hist$raw$calc %>%
		bind_rows(
			.,
			filter(hist$raw$fred, !varname %in% unique(.$varname)),
			filter(hist$raw$yahoo, !varname %in% unique(.$varname)),
			filter(hist$raw$bloom, !varname %in% unique(.$varname)),
			filter(hist$raw$afx, !varname %in% unique(.$varname)),
			filter(hist$raw$ecb, !varname %in% unique(.$varname)),
			filter(hist$raw$boe, !varname %in% unique(.$varname))
		)

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
	# 3 monthly data points are available -this can still result in premature quarterly calcs if the last
	# month value has been agrgegated before that last month was finished
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
				.[, c('varname', 'date') := list(x$varname[[1]], date = x$this_quarter[[1]])] %>%
				.[count == 3]
		) %>%
		rbindlist(.) %>%
		as_tibble(.) %>%
		transmute(., varname, freq = 'q', date, vdate, value)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q'))

	hist$agg <<- hist_agg
})


# Split & Transform ----------------------------------------------------------

## 1. Split By Vintage Date ----------------------------------------------------------
local({

	message(str_glue('*** Splitting By Vintage Date | {format(now(), "%H:%M")}'))

	# Check min dates
	message('***** Variables Dates:')
	hist$agg %>%
		group_by(., varname) %>%
		summarize(., min_dt = min(date)) %>%
		arrange(., desc(min_dt)) %>%
		print(., n = 5)

	# Get table indicating which variables can have their historical data be revised well after the fact
	# For these variables, not necessary to build out a full history for each vdate for later stationary transforms
	# This saves significant time
	# Only include variables which definitely have final vintage data within 30 days of original data
	revisable =
		variable_params %>%
		mutate(
			.,
			hist_revisable =
			   	!dispgroup %in% c('Interest_Rates', 'Stocks_and_Commodities') &
				!hist_source_freq %in% c('d', 'w')
			) %>%
		select(., varname, hist_revisable) %>%
		as.data.table(.)

	last_obs_by_vdate =
		hist$agg %>%
		as.data.table(.) %>%
		split(., by = c('varname', 'freq')) %>%
		lapply(., function(x)  {

			# message(str_glue('**** Getting last vintage dates for {x$varname[[1]]}'))
			# For every vintage date within last 6 months, get last observation for every past date
			# This creates lots of duplicates, especially for monthly variables - clean them later
			# after doing stationary transforms
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
				.[, date := as_date(date)] %>%
				# For non revisable table, only minimal data is  needed for calculating stationary transformations
				merge(., revisable, by = 'varname', keep.x = T) %>%
				.[hist_revisable == T | (hist_revisable == F & vdate <= date + days(60))] %>%
				select(., -hist_revisable)
			return(last_obs_for_all_vdates)
		}) %>%
		rbindlist(.)

	hist$base <<- last_obs_by_vdate
})

## 2. Add Stationary Transformations ----------------------------------------------------------
local({

	message(str_glue('*** Adding Stationary Transforms | {format(now(), "%H:%M")}'))

	# Microbenchmark @ 1.8s per 100k rows
	stat_final =
		copy(hist$base) %>%
		merge(., variable_params[, c('varname', 'd1', 'd2')], by = c('varname'), all = T) %>%
		melt(
			.,
			id.vars = c('varname', 'freq', 'vdate', 'date', 'value'),
			variable.name = 'form',
			value.name = 'transform'
			) %>%
		.[transform != 'none'] %>%
		.[order(varname, freq, form, vdate, date)] %>%
		.[,
			value := frollapply(
				value,
				n = 2,
				FUN = function(z) { # Z is a vector of length equal to the window size, asc order
					if (transform[[1]] == 'base') z[[2]]
					else if (transform[[1]] == 'log') log(z[[2]])
					else if (transform[[1]] == 'dlog') log(z[[2]]/z[[1]])
					else if (transform[[1]] == 'diff1') z[[2]] - z[[1]]
					else if (transform[[1]] == 'pchg') (z[[2]]/z[[1]] - 1) * 100
					else if (transform[[1]] == 'apchg') ((z[[2]]/z[[1]])^{if (freq[[1]] == 'q') 4 else 12} - 1) * 100
					else stop ('Error')
					},
				fill = NA
				),
			by = c('varname', 'freq', 'form', 'vdate')
			] %>%
		.[, transform := NULL] %>%
		bind_rows(., copy(hist$base)[, form := 'base']) %>%
		na.omit(.)

	stat_final_last =
		stat_final %>%
		group_by(., varname) %>%
		mutate(., max_vdate = max(vdate)) %>%
		filter(., vdate == max(vdate)) %>%
		select(., -max_vdate) %>%
		ungroup(.) %>%
		as.data.table(.)

	hist$flat <<- stat_final
	hist$flat_last <<- stat_final_last
})

## 3. Create Monthly/Quarterly Matrices ----------------------------------------------------------
local({

	# message(str_glue('*** Creating Wide Matrices | {format(now(), "%H:%M")}'))
	#
	# wide =
	# 	hist$flat %>%
	# 	split(., by = 'freq', keep.by = F) %>%
	# 	lapply(., function(x)
	# 		split(x, by = 'form', keep.by = F) %>%
	# 			lapply(., function(y)
	# 				split(y, by = 'vdate', keep.by = T) %>%
	# 					lapply(., function(z) {
	# 						# message(z$vdate[[1]])
	# 						dcast(select(z, -vdate), date ~ varname, value.var = 'value') %>%
	# 							.[, date := as_date(date)] %>%
	# 							.[order(date)] %>%
	# 							as_tibble(.)
	# 					})
	# 			)
	# 	)
	#
	# wide_last <<- lapply(wide, function(x) lapply(x, function(y) tail(y, 1)[[1]]))
	#
	# hist$wide <<- wide
	# hist$wide_last <<- wide_last
})

## 4. Strip Duplicates ---------------------------------------------------------------------
local({

	message(str_glue('*** Stripping Vintage Date Dupes | {format(now(), "%H:%M")}'))

	hist_stripped =
		copy(hist$flat) %>%
		# Rleid changes when value changes (by vdate)
		.[order(vdate, date), value_runlength := rleid(value), by = c('varname', 'freq', 'date', 'form')] %>%
		# Get first rleid only for each group -
		.[, .SD[which.min(vdate)], by = c('varname', 'freq', 'date', 'form', 'value_runlength')] %>%
		.[, value_runlength := NULL]

	hist$flat_final <<- hist_stripped
})

# Finalize ----------------------------------------------------------------

## 1. SQL ----------------------------------------------------------------
local({

	message(str_glue('*** Sending Historical Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_hist_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		hist$flat_final %>%
		select(., vdate, form, freq, varname, date, value) %>%
		as_tibble(.) %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'forecast_hist_values',
				'ON CONFLICT (vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_hist_values')$count)
	message('***** Rows Added: ', final_count - initial_count)

	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'composite-model', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
})

## 2. Close Connections ----------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
