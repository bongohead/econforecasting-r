#' This generates composite forecasts for monthly datasets!
#'
#' It ingests forecasts (both monthly and quarterly) as well as historical data
#   for variables with monthly history.

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-monthly-dlog-stacking'
EF_DIR = Sys.getenv('EF_DIR')
TRAIN_VDATE_START = '2015-01-01'
VARNAMES = c('unemp', 'ffr')

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
library(lubridate)
library(xgboost)

## Load Connection Info ----------------------------------------------------------
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
run_id = log_start_in_db(db, JOB_NAME, 'composite-model')
releases = list()
hist = list()

## Load Variable Defs ----------------------------------------------------------
variable_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_variables'))
release_params = as_tibble(dbGetQuery(db, 'SELECT * FROM forecast_hist_releases'))

# Import Test Backdates ----------------------------------------------------------

## Import Monthly Historical Data ----------------------------------------------------------
local({

	# Only keep EARLIEST release for each historical value
	# This results in date being the unique row identifier
	hist_data_d1 = tbl(db, sql(str_glue(
		"
		WITH t0 AS (
			-- Pivot out d1 and d2
			SELECT
				t1.vdate, t1.freq, t1.varname, t1.date,
				t1.value AS d1, t2.value AS d2
			FROM (SELECT * FROM forecast_hist_values_v2 WHERE form = 'd1') t1
			LEFT JOIN (SELECT * FROM forecast_hist_values_v2 WHERE form = 'd2') t2 ON (
				t1.vdate = t2.vdate
				AND t1.freq = t2.freq
				AND t1.varname = t2.varname
				AND t1.date = t2.date
			)
		)
		-- Now aggregate out vdates to get the latest freq x date x varname combo
		SELECT
			date, varname, MIN(vdate) as vdate, FIRST(d1, vdate) as value
		FROM t0
		WHERE varname IN ({v}) AND freq = 'm'
		GROUP BY varname, freq, date
		ORDER BY varname, freq, date
		",
		v = paste0(paste0('\'', VARNAMES, '\''), collapse = ',')
		))) %>%
		collect(.) %>%
		arrange(., varname, date)

	# Apply stationary transformations
	# TBD: see composite-model-get--hist for significantly faster & more memory-efficient stationary transforms
	hist_data_st =
		hist_data_d1 %>%
		group_split(., varname) %>%
		map_dfr(., function(x)
			x %>%
				arrange(., date) %>%
				mutate(., value = diff1(value)) %>%
				tail(., -1)
			)

	hist_data_d1 <<- hist_data_d1
	hist_data_st <<- hist_data_st
})

## Import Forecasts ----------------------------------------------------------
local({

	forecast_data_d1_all = tbl(db, sql(str_glue(
		"
		SELECT val.varname, val.forecast, val.vdate, val.date, val.d1 AS value, val.freq
		FROM forecast_values_v2_all val
		INNER JOIN forecast_variables v ON v.varname = val.varname
		WHERE
			v.varname IN ({v}) AND freq IN ('m', 'q')
			AND vdate >= '{TRAIN_VDATE_START}'
		ORDER BY val.varname, val.vdate, val.date
		",
		v = paste0(paste0('\'', VARNAMES, '\''), collapse = ',')
		))) %>%
		collect(.)

	# If quarterly, guess monthly forecasts via interpolation of missing vintages
	forecast_data_d1_q_raw =
		forecast_data_d1_all %>%
		filter(., freq == 'q') %>%
		select(., -freq) %>%
		rename(., forecast_quarter_date = date)

	forecast_data_d1_q_months =
		forecast_data_d1_q_raw %>%
		group_split(., varname, vdate, forecast) %>%
		lapply(., function(x)
			tibble(forecast_month_date = seq(
				from = min(x$forecast_quarter_date),
				to = max(x$forecast_quarter_date) + months(2),
				by = '1 month'
				)) %>%
				bind_cols(varname = x$varname[[1]], vdate = x$vdate[[1]], forecast = x$forecast[[1]], .)
			) %>%
		list_rbind(.) %>%
		mutate(
			.,
			forecast_quarter_date = floor_date(forecast_month_date, 'quarter'),
			month_n = interval(forecast_quarter_date, forecast_month_date) %/% months(1) + 1
		)

	forecast_data_d1_q =
		forecast_data_d1_q_months %>%
		left_join(., forecast_data_d1_q_raw, by = c('varname', 'forecast', 'vdate', 'forecast_quarter_date')) %>%
		left_join(
			.,
			transmute(
				hist_data_d1,
				varname, hist_month_date = date, hist_quarter_date = floor_date(date, 'quarter'),
				hist_value = value, hist_vdate = vdate
				),
			join_by(varname, forecast_month_date == hist_month_date)
		) %>%
		# Add a column for a hist val for the month if known
		mutate(., known_hist_val = ifelse(vdate >= hist_vdate, hist_value, NA)) %>%
		# Now pivot such that each row uniquely identifies a quarter forecast (for a given vdate x varname)
		# with columns identifying histories for month 0, month 1, month 2
		pivot_wider(
			.,
			id_cols = c(varname, vdate, forecast, forecast_quarter_date, value),
			names_from = month_n,
			values_from = known_hist_val,
			names_prefix = 'known_month_'
			) %>%
		# Now interpolate missing values
		mutate(., interp_vals = map(purrr::transpose(.), function(r) {
			is_na_hist = is.na(c(r$known_month_1, r$known_month_2, r$known_month_3))
			res =  {
				# If no hist
				if (all(is_na_hist)) c(r$value, r$value, r$value)
				# If first hist
				else if (!is_na_hist[1] & is_na_hist[2] & is_na_hist[3])
					c(r$known_month_1, (3 * r$value - r$known_month_1)/2, (3 * r$value - r$known_month_1)/2)
				# If first 2 hist
				else if (!is_na_hist[1] & !is_na_hist[2] & is_na_hist[3])
					c(r$known_month_1, r$known_month_2, r$value * 3 - r$known_month_1 - r$known_month_2)
				# If all hist
				else if (!is_na_hist[1] & !is_na_hist[2] & !is_na_hist[3])
					c(r$known_month_1, r$known_month_2, r$known_month_3)
				else stop('Data error')
			}
			return(set_names(res, paste0('interp_month_', 1:3)))
		})) %>%
		unnest_wider(., interp_vals) %>%
		select(., -starts_with('known')) %>%
		pivot_longer(
			.,
			cols = starts_with('interp'),
			values_to = 'forecast_month_val',
			names_to = 'forecast_month_date'
			) %>%
		mutate(
			.,
			forecast_month_date = forecast_quarter_date + months(as.integer(str_sub(forecast_month_date, -1)) - 1)
			)
		# Now add splinal smoothers

	forecast_data_d1 = bind_rows(
		forecast_data_d1_all %>%
			filter(., freq == 'm') %>%
			select(., -freq),
		forecast_data_d1_q %>%
			transmute(
				.,
				varname,
				forecast,
				vdate,
				date = forecast_month_date,
				value = forecast_month_val
			)
		)


	## Stationary Transformers
	forecast_data_st =
		forecast_data_d1 %>%
		group_split(., varname, vdate) %>%
		imap(., .progress = T, function(x, i) {

			x_varname = x$varname[[1]]

			hist_bind =
				hist_data_d1 %>%
				filter(., date == min(x$date) - months(1) & varname == x_varname)

			if (nrow(hist_bind) != 1) {
				message('Error on iteration ', i)
				print(x)
				return(NULL)
			}

			bind_rows(hist_bind, x) %>%
				arrange(., date) %>%
				mutate(., value = diff1(value)) %>%
				tail(., -1) %>%
				return(.)
		}) %>%
		compact(.) %>%
		list_rbind(.)


	forecast_data_d1 <<- forecast_data_d1
	forecast_data_st <<- forecast_data_st
})

## Import Release Schedule ----------------------------------------------------------
local({

	# We can guess release times that are upcoming
	# This will be later used to determine which releases are upcoming but not yet released
	release_lag =
		hist_data_d1 %>%
		group_by(., varname) %>%
		summarize(., release_lag = ceiling(as.integer(mean(vdate - date), 'days')), .groups = 'drop')

	# Only include data not already forecasted
	# release_data =
	monthly_possible_releases = expand_grid(
		date = seq(
			from = floor_date(today(), 'months') - years(1),
			to = floor_date(today(), 'months') + years(10),
			by = '1 month'
			),
		varname = unique(forecast_data_d1$varname)
		)

	release_data =
		monthly_possible_releases %>%
		anti_join(., hist_data_d1, by = c('varname', 'date')) %>%
		left_join(., release_lag, by = 'varname') %>%
		mutate(., hist_vdate = date + release_lag) %>%
		select(., -release_lag)

	release_data <<- release_data
})

# Additional Models ----------------------------------------------------------

## Prep Data (Monthly) ----------------------------------------------------------
model_data =
	hist_data_d1 %>%
	as.data.table(.) %>%
	split(., by = c('varname')) %>%
	lapply(., function(x)  {
		x %>%
			.[order(vdate)] %>%
			dcast(., varname + vdate ~  date, value.var = 'value') %>%
			.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
			melt(
				.,
				id.vars = c('varname', 'vdate'),
				value.name = 'value',
				variable.name = 'date',
				na.rm = T
			) %>%
			.[, date := as_date(date)]
	}) %>%
	rbindlist(.)

## ARIMA Forecasts (Monthly) ----------------------------------------------------------
arima_forecasts =
	model_data %>%
	split(., by = c('varname', 'vdate')) %>%
	keep(., ~ nrow(.) >= 60) %>%
	lapply(., function(x) {
		input_df =
			x[order(date)] %>%
			.[, value.l1 := shift(value, 1, type = 'lag')] %>%
			.[, constant := 1] %>%
			.[2:nrow(.)]

		input_df_cleaned = input_df #[!year(date) == 2020]

		x_mat = as.matrix(input_df_cleaned[, c('value.l1', 'constant')])

		y_mat = as.matrix(input_df_cleaned[, 'value'])

		coef_mat = solve(t(x_mat) %*% x_mat) %*% t(x_mat) %*% y_mat

		## 20 period forecasts
		data.table(
			varname = x$varname[[1]],
			vdate = x$vdate[[1]],
			date = seq(tail(input_df$date, 1), length.out = 61, by = '1 month')[2:61],
			value = accumulate(1:60, function(accum, x)
				accum * coef_mat[[1, 1]] + coef_mat[[2, 1]],
				.init = tail(input_df$value, 1)
			)[2:61]
		)
	}) %>%
	rbindlist(.) %>%
	.[, forecast := 'arima']

## Constant Forecasts (Monthly) ----------------------------------------------------------
constant_forecasts =
	model_data %>%
	split(., by = c('varname', 'vdate')) %>%
	keep(., ~ nrow(.) >= 5) %>%
	lapply(., function(x) {
		input_df = x[order(date)]

		## 20 period forecasts
		data.table(
			varname = x$varname[[1]],
			vdate = x$vdate[[1]],
			date = seq(tail(x$date, 1), length.out = 61, by = '1 month')[2:61],
			value = median(x$value)
		)
	}) %>%
	rbindlist(.) %>%
	.[, forecast := 'constant']

## MA Forecasts (Monthly) ----------------------------------------------------------
ma_forecasts =
	model_data %>%
	split(., by = c('varname', 'vdate')) %>%
	keep(., ~ nrow(.) >= 5) %>%
	lapply(., function(x) {
		input_df = x[order(date)]

		## 20 period forecasts
		data.table(
			varname = x$varname[[1]],
			vdate = x$vdate[[1]],
			date = seq(tail(x$date, 1), length.out = 61, by = '1 month')[2:61],
			value = mean(tail(input_df$value, 12))
		)
	}) %>%
	rbindlist(.) %>%
	.[, forecast := 'ma']


forecast_data_final = bind_rows(forecast_data_d1, arima_forecasts, constant_forecasts, ma_forecasts)


# Combine Models ----------------------------------------------------------

## Prep Train Data ----------------------------------------------------------
train_data_0 =
	merge(
		as.data.table(forecast_data_final),
		rename(as.data.table(hist_data_d1), hist_vdate = vdate, hist_value = value),
		by = c('varname', 'date'),
		all = FALSE,
		allow.cartesian = TRUE
	) %>%
	as_tibble(.) %>%
	filter(., as.numeric(hist_vdate - vdate, 'days') >= 1 & vdate >= TRAIN_VDATE_START)
####
#' Fix realdate calculation in both this and and test version!

####
train_data =
	train_data_0 %>%
	group_by(., varname) %>%
	group_split(.) %>%
	map(., .progress = T, function(x) {
		res =
			# Create grid of 300 days trailing before each obs release date for each forecast
			expand_grid(
				forecast = unique(x$forecast),
				days_before_release = 1:300,
				x %>%
					group_by(., date, hist_vdate) %>%
					summarize(., hist_vdate = unique(hist_vdate), hist_value = unique(hist_value), .groups = 'drop') %>%
					arrange(., date),
				) %>%
			mutate(., realdate = date - days(days_before_release)) %>%
			# Now join the closest forecast to each date
			left_join(
				.,
				transmute(x, forecast, date, forecast_vdate = vdate, forecast_value = value),
				join_by(forecast, date, closest(realdate >= forecast_vdate))
				) %>%
			mutate(
				.,
				forecast_age = as.integer(realdate - forecast_vdate),
				varname = x$varname[[1]]
				)

		return(res)
	}) %>%
	list_rbind(.) %>%
	na.omit(.) %>%
	pivot_wider(
		.,
		id_cols = c('varname', 'date', 'hist_vdate', 'hist_value', 'realdate', 'days_before_release'),
		names_from = forecast,
		values_from = c(forecast_value, forecast_age)
	)

train_data %>%
	filter(., date == max(date)) %>%
	print(.)

## Prep Test Data ----------------------------------------------------------

# Get data that is forecasted & has a prospective release date, but no historical data
test_data_0 =
	release_data %>%
	left_join(., forecast_data_final, by = c('date', 'varname'), multiple = 'all')

test_data =
	test_data_0 %>%
	group_by(., varname) %>%
	group_split(.) %>%
	map(., .progress = T, function(x) {
		res =
			# Create grid of 300 days trailing before each obs release date for each forecast
			expand_grid(
				forecast = unique(x$forecast),
				days_before_release = 1:300,
				x %>%
					group_by(., date, hist_vdate) %>%
					summarize(., hist_vdate = unique(hist_vdate), .groups = 'drop') %>%
					arrange(., date),
			) %>%
			mutate(., realdate = date - days(days_before_release)) %>%
			# Now join the closest forecast to each date
			left_join(
				.,
				transmute(x, forecast, date, forecast_vdate = vdate, forecast_value = value),
				join_by(forecast, date, closest(realdate >= forecast_vdate))
			) %>%
			mutate(
				.,
				forecast_age = as.integer(realdate - forecast_vdate),
				varname = x$varname[[1]]
			)

		return(res)
	}) %>%
	list_rbind(.) %>%
	na.omit(.) %>%
	pivot_wider(
		.,
		id_cols = c('varname', 'date', 'hist_vdate', 'realdate', 'days_before_release'),
		names_from = forecast,
		values_from = c(forecast_value, forecast_age)
	)

# test_data =
# 	# TBD: Get data that is forecasted & has a prospective release date, but no historical data
# 	anti_join(
# 		inner_join(forecast_data_final, release_data, by = 'date'),
# 		rename(hist_data, hist_vdate = vdate, hist_value = value),
# 		by = c('varname', 'date', 'freq')
# 	) %>%
# 	filter(., date >= today() - years(1)) %>%
# 	mutate(., days_before_release = as.numeric(release_date - vdate, 'days'))
#
#
#
# %>%
# 	as.data.table(.) %>%
# 	split(., by = c('date', 'varname')) %>%
# 	lapply(., function(x)
# 		x %>%
# 			dcast(., days_before_release ~ forecast, value.var = 'value') %>%
# 			merge(
# 				data.table(days_before_release = seq(max(.$days_before_release), 1, -1)),
# 				.,
# 				by = 'days_before_release',
# 				all = TRUE
# 			) %>%
# 			.[order(-days_before_release)] %>%
# 			.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
# 			melt(., id.vars = 'days_before_release', value.name = 'value', variable.name = 'forecast', na.rm = T) %>%
# 			.[,
# 				c('varname', 'date', 'release_date') :=
# 					list(x$varname[[1]], x$date[[1]], x$release_date[[1]])
# 			]
# 	) %>%
# 	rbindlist(.) %>%
# 	# .[, vdate := release_date - days_before_release] %>%
# 	dcast(., varname + date + release_date + days_before_release ~ forecast, value.var = 'value')


## Train Model ----------------------------------------------------------
train_varnames =
	# unique(hist_data$varname)
	VARNAMES

trees = lapply(train_varnames, function(this_varname) {

	input_df =
		train_data %>%
		filter(., varname == this_varname)
		# .[!year(date) %in% c(2020)]
		# .[!date %in% as_date(c('2020-04-01', '2020-07-01'))]

	x_mat =
		input_df %>%
		select(
			.,
			days_before_release,
			starts_with('forecast_value'),
			starts_with('forecast_date')
		) %>%
		mutate(
			.,
			days_before_release = ifelse(days_before_release >= 1000, 1000, days_before_release),
		) %>%
		as.matrix(.)

	tree = xgboost::xgboost(
		data = x_mat,
		label = input_df$hist_value,
		verbose = 2,
		max_depth = 10,
		print_every_n = 50,
		nthread = 6,
		nrounds = 1000,
		objective = 'reg:squarederror',
		booster = 'gbtree'
		#,
		# monotone_constraints = '(0,1,1,1,1,1,1)'
	)

	return(list(
		tree = tree,
		coefs = colnames(x_mat)
		))
	}) %>%
	set_names(., train_varnames)


## Predict ----------------------------------------------------------
# Predicts values from test data,
# i.e. dates that haven't been forecasted yet at all
test_results = lapply(train_varnames, function(this_varname) {

	message(this_varname)

	oos_values =
		predict(
			trees[[this_varname]]$tree,
			test_data %>%
				filter(., varname == this_varname) %>%
				select(., all_of(trees[[this_varname]]$coefs)) %>%
				as.matrix(.)
			)

	predicted_values =
		test_data %>%
		filter(., varname == this_varname) %>%
		mutate(., vdate = realdate, predict = oos_values) %>%
		filter(., vdate <= today())

	# predicted_values %>%
	# 	# transmute(., varname, date, release_date, days_before_release, vdate, predict) %>%
	# 	filter(., varname %in% c('gdp', 'pce') & date == min(date)) %>%
	# 	pivot_wider(., names_from = varname, values_from = predict) %>%
	# 	View(.)
	return(predicted_values)
	}) %>%
	bind_rows(.)

test_results %>%
	filter(., varname == 'unemp') %>%
	select(., vdate, date, predict) %>%
	pivot_wider(., names_from = 'date', values_from = 'predict') %>%
	arrange(., desc(vdate)) %>%
	print(., n = 100)

## Reverse-Transform ----------------------------------------------------------
test_results_d1 =
	test_results %>%
	select(., vdate, date, predict, varname) %>%
	# Below line for testing due to speed
	filter(., vdate >= today()) %>%
	group_split(., vdate, varname) %>%
	imap(., function(x, i) {

		hist_value = filter(hist_data_d1, varname == x$varname[[1]], freq == 'm' & date == min(x$date) - months(1))$value

		if (length(hist_value) != 1) {
			stop('Error on index ', i)
		}

		x %>%
			mutate(., predict = undiff(predict, 1, hist_value)) %>%
			return(.)
	}) %>%
	bind_rows(.)

test_results_d1 %>%
	filter(., varname == 'ffr') %>%
	select(., vdate, date, predict) %>%
	pivot_wider(., names_from = 'date', values_from = 'predict') %>%
	arrange(., desc(vdate)) %>%
	print(., n = 100)  %>%
	View(.)

# Finalize ----------------------------------------------------------

## SQL ----------------------------------------------------------
local({

	forecast_values =
		test_results %>%
		transmute(., forecast = 'comp', form = 'd1', vdate, freq = 'q', varname, date, value = predict)

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		forecast_values %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'forecast_values',
				'ON CONFLICT (forecast, vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_values')$count)
	message('***** Rows Added: ', final_count - initial_count)

	tribble(
		~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
		JOB_NAME, 'composite-model', today(), 'job-success',
		toJSON(list(rows_added = final_count - initial_count))
	) %>%
		create_insert_query(
			.,
			'job_logs',
			'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
})

## Close Connections ----------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
