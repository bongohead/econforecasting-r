#' Ad-hoc request to get SOFR & FFR confidence intervals
#' It requires forecast_values and forecast_hist_values are already built
#' - Forecast values must first be run by their respective modules
#' - Forecast_hist_values must be run by composite-model-get-hist

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-calculate-quantiles'
EF_DIR = Sys.getenv('EF_DIR')

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

## Load Data ----------------------------------------------------------
local({
	
	# variable_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_variables'))
	forecast_values = as_tibble(dbGetQuery(
		db,
		"SELECT forecast, vdate, freq, varname, date, value
		FROM forecast_values
		WHERE form = 'd1'
			--AND varname = 'ffr'"
		))
	
	# Select unique date-level data
	hist_values = as_tibble(dbGetQuery(
		db,
		"SELECT latest_hist_vdate, first_hist_vdate, freq, date, varname, latest_hist_value
		FROM
		(
			SELECT 
				freq, form, date, varname, MAX(vdate) as latest_hist_vdate,
				last(value, vdate) as latest_hist_value, -- Get value corresponding to latest vdate for each date
				MIN(vdate) AS first_hist_vdate
			FROM forecast_hist_values
			WHERE 
				form = 'd1'
				--AND varname = ANY('{ffr}'::VARCHAR[])
			GROUP BY varname, form, freq, date
			ORDER BY varname, form, freq, date
		) AS a"
		))
	
	forecast_values <<- forecast_values
	hist_values <<- hist_values
})

# Load Data ----------------------------------------------------------

## Calculate Historical Errors ----------------------------------------------------------
local({
	
	error_values =
		forecast_values %>%
		inner_join(., select(hist_values, -freq), by = c('date', 'varname')) %>%
		mutate(., error = latest_hist_value - value, days_to_latest_release = as.numeric(vdate - latest_hist_vdate))
		
	error_values <<- error_values
})


## Error 1: Structural ----------------------------------------------------------
forecast_values %>% 
	filter(., varname %in% c('ffr', 'sofr') & forecast == 'int' & freq == 'm') %>% 
	filter(., vdate == max(vdate)) %>%
	mutate(., days_to_latest_release = as.numeric(vdate - ceiling_date(date, 'months'))) %>%
	purrr::transpose(.) %>%
	lapply(., function(x)
		error_xs %>%
			filter(., days_to_latest_release == x$days_to_latest_release) %>%
			.$error
	)

error_xs =
	error_values %>%
	filter(., varname %in% c('sofr', 'ffr') & forecast == 'int' & freq == 'm') %>%
	filter(., days_to_latest_release <= 0) %>%
	group_by(., varname, forecast, days_to_latest_release) %>%
	summarize(
		.,
		q_10 = quantile(error, .1),
		q_90 = quantile(error, .9),
		n_vintages = n(),
		var = var(error),
		.groups = 'drop'
		) %>%
	filter(., n_vintages >= 3)
# 
# forecast_values %>%
# 	filter(., varname %in% c('ffr', 'sofr') & forecast == 'int' & freq == 'm') %>%
# 	filter(., vdate == max(vdate)) %>%
# 	mutate(., days_to_latest_release = as.numeric(vdate - ceiling_date(date, 'months'))) %>%
# 	left_join(., error_xs, by = c('varname', 'forecast', 'days_to_latest_release')) %>%
# 	mutate(., c_10 = value + qnorm(.1, 0, .5), c_90 = value + qnorm(.9, 0, .5)) %>%
# 	filter(., varname %in% c('sofr', 'ffr')) %>%
# 	select(., forecast, vdate, date, value, c_10, c_90) %>%
# 	arrange(., date) %>%
# 	ggplot(.) + geom_line(aes(x = date, y = value))  + geom_line(aes(x = date, y = c_10))
	

# forecast_values %>%
# 	filter(., varname %in% c('ffr', 'sofr') & forecast == 'int' & freq == 'm') %>%
# 	filter(., vdate == max(vdate)) %>%
# 	mutate(., days_to_latest_release = as.numeric(vdate - ceiling_date(date, 'months'))) %>%
# 	left_join(., error_xs, by = c('varname', 'forecast', 'days_to_latest_release')) %>% rowwise() %>% mutate(., q_10 = qnorm(.05, value, sqrt(var)), q_90 = qnorm(.9, value, sqrt(var))) %>% View(.)


forecast_values %>%
	filter(., varname %in% c('ffr', 'sofr') & forecast == 'int' & freq == 'm') %>%
	filter(., vdate == max(vdate)) %>%
	mutate(., days_to_latest_release = as.numeric(vdate - ceiling_date(date, 'months'))) %>%
	left_join(., error_xs, by = c('varname', 'forecast', 'days_to_latest_release')) %>%
	mutate(., var = zoo::na.locf(var)) %>%
	rowwise() %>% 
	mutate(., ci_10 = qnorm(.1, value, sqrt(var)*.9), ci_90 = qnorm(.9, value, sqrt(var))) %>% 
	mutate(., ci_05 = qnorm(.05, value, sqrt(var)*.9), ci_95 = qnorm(.95, value, sqrt(var))) %>% 
	transmute(., vdate, varname, freq, date, mean = value, ci_10, ci_90, ci_05, ci_95) %>%
	pivot_wider(., id_cols = 'date', names_from = c('varname'), values_from = c('mean', 'ci_10', 'ci_05', 'ci_90', 'ci_95')) %>%
	write_csv(., 'benchmark_forecast_distributions_20220706.csv')

# 
# error_xs %>%
# 	ggplot(.) + geom_point(aes(x = days_to_latest_release, y = error))
## Error 2 ----------------------------------------------------------

# calculate_price = function(S, K, r, T, sig, type) {
# 	
# 	if(type=="C"){
# 		d1 <- (log(S/K) + (r + sig^2/2)*T) / (sig*sqrt(T))
# 		d2 <- d1 - sig*sqrt(T)
# 		
# 		value <- S*pnorm(d1) - K*exp(-r*T)*pnorm(d2)
# 		return(value)}
# 	
# 	if(type=="P"){
# 		d1 <- (log(S/K) + (r + sig^2/2)*T) / (sig*sqrt(T))
# 		d2 <- d1 - sig*sqrt(T)
# 		
# 		value <-  (K*exp(-r*T)*pnorm(-d2) - S*pnorm(-d1))
# 		return(value)}
# }


error_values %>%
	filter(., forecast == 'int' & varname == 'ffr') %>%
	filter(., date %in% sample(unique(.$date), size = 5)) %>%
	ggplot(.) +
	geom_line(aes(x = vdate, y = error, color = as.factor(date)))

# Randomly sample 10 ffr plots

## Visualizations & Checks ----------------------------------------------------------
local({
	
	# Drop freq on join
	error_ttr_plots =
		error_values
		filter(., days_to_latest_release <= 0) %>%
		group_by(., varname, forecast, days_to_latest_release) %>%
		summarize(., mae = mean(abs(error)), n_vintages = n(), .groups = 'drop') %>%
		filter(., n_vintages >= 3) %>%
		group_split(., varname) %>%
		set_names(., map_chr(., ~ .$varname[[1]])) %>%
		lapply(., function(x)
			x %>%
				ggplot(.) +
				geom_point(aes(x = days_to_latest_release, y = mae, color = forecast))
		)
	
	error_1y_plots =
		forecast_values %>%
		inner_join(., select(hist_values, -freq), by = c('date', 'varname')) %>%
		mutate(., error = latest_hist_value - value, days_to_latest_release = as.numeric(vdate - latest_hist_vdate)) %>%
		filter(., days_to_latest_release >= -390 & days_to_latest_release <= -360) %>%
		# group_by(., varname, forecast, date) %>%
		group_split(., varname) %>%
		set_names(., map_chr(., ~ .$varname[[1]])) %>%
		lapply(., function(x)
			x %>%
				ggplot(.) +
				geom_point(aes(x = date, y = error, color = forecast), alpha = .5) + 
				labs(title = 'Forecast error by obs date, for approx. 1 year old forecasts')
		)
	

	print(error_ttr_plots$ffr)
	print(error_ttr_plots$sofr)
	print(error_ttr_plots$t10y)
	print(error_ttr_plots$gdp)
	
})

## 1. Split Histories By Vintage Date ----------------------------------------------------------
local({

	message(str_glue('*** Splitting By Vintage Date | {format(now(), "%H:%M")}'))

	# Check min dates
	message('***** Hist Dates:')
	hist_values %>%
		group_by(., varname) %>%
		summarize(., min_dt = min(date)) %>%
		arrange(., desc(min_dt)) %>%
		print(., n = 5)

	last_obs_by_vdate =
		hist_values %>%
		as.data.table(.) %>%
		split(., by = c('varname', 'freq')) %>%
		lapply(., function(x)  {

			# message(str_glue('**** Getting last vintage dates for {x$varname[[1]]}'))
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
				# Calculate 5 year forecast
				.[vdate <= date + years(5)]
			
			return(last_obs_for_all_vdates)
		}) %>%
		rbindlist(.)

	d1_data <<- last_obs_by_vdate
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
