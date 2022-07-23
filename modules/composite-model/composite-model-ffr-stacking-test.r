#' This gets historical data for website

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-ffr-stacking'
EF_DIR = Sys.getenv('EF_DIR')
TRAIN_VDATE_START = '2015-01-01'

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
library(xgboost)

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

# Import Test Backdates ----------------------------------------------------------

## Import Historical Data ----------------------------------------------------------
hist_data = tbl(db, sql(
	"SELECT val.varname, val.vdate, val.date, val.value
	FROM forecast_hist_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE v.varname IN ('ffr', 'sofr') AND freq IN 'm' AND form = 'd1'"
)) %>%
	collect(.) %>%
	# Only keep initial release for each historical value
	# This results in date being the unique row identifier
	group_by(., varname, date) %>%
	filter(., vdate == min(vdate)) %>%
	ungroup(.) %>%
	arrange(., varname, date)

## Import Forecasts ----------------------------------------------------------
forecast_data = tbl(db, sql(
	"SELECT val.varname, val.forecast, val.vdate, val.date, val.value
	FROM forecast_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE v.varname IN ('ffr', 'sofr') AND freq IN ('m', 'q') AND form = 'd1'"
)) %>%
	collect(.)

## Import Forecasts ----------------------------------------------------------
release_data =
	tibble(date = seq(from = as_date('2015-01-01'), to = ceiling_date(today(), 'month') + years(10), by = '1 month')) %>%
	mutate(., release_date = date)

# Additional Models ----------------------------------------------------------

## Prep Data ----------------------------------------------------------
model_data =
	hist_data %>%
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


forecast_data_final = bind_rows(forecast_data, arima_forecasts, constant_forecasts, ma_forecasts)

## Dynamic Factor Model Forecasts (TBD) ----------------------------------------------------------


## TVP-AR (TBD) ----------------------------------------------------------



# Combine Models ----------------------------------------------------------

## Prep Train Data ----------------------------------------------------------
train_data =
	merge(
		as.data.table(forecast_data_final),
		rename(as.data.table(hist_data), hist_vdate = vdate, hist_value = value),
		by = c('varname', 'date'),
		all = FALSE,
		allow.cartesian = TRUE
	) %>%
	.[, c('error', 'days_before_release') := list(hist_value - value, as.numeric(hist_vdate - vdate))] %>%
	.[days_before_release >= 1] %>%
	.[vdate >= TRAIN_VDATE_START] %>%
	# .[, c('hist_vdate', 'hist_value') := NULL] %>%
	# Only keep initial release for each historical value
	# For each obs date, get the forecast & vdates
	split(., by = c('date', 'varname')) %>%
	lapply(., function(x)
		x %>%
			dcast(., days_before_release ~ forecast, value.var = 'value') %>%
			merge(
				data.table(days_before_release = seq(max(.$days_before_release), 1, -1)),
				.,
				by = 'days_before_release',
				all = TRUE
			) %>%
			.[order(-days_before_release)] %>%
			.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
			melt(., id.vars = 'days_before_release', value.name = 'value', variable.name = 'forecast', na.rm = T) %>%
			.[,
				c('varname', 'date', 'hist_vdate', 'hist_value') :=
					list(x$varname[[1]], x$date[[1]], x$hist_vdate[[1]], x$hist_value[[1]])
			]
	) %>%
	rbindlist(.) %>%
	dcast(., varname + date + hist_vdate + hist_value + days_before_release ~ forecast, value.var = 'value')

train_data %>%
	filter(., date == max(date)) %>%
	print(.)

## Prep Test Data ----------------------------------------------------------
test_data =
	# TBD: Get data that is forecasted & has a prospective release date, but no historical data
	anti_join(
		inner_join(forecast_data_final, release_data, by = 'date'),
		rename(hist_data, hist_vdate = vdate, hist_value = value),
		by = c('varname', 'date')
	) %>%
	filter(., date >= '2015-01-01') %>%
	mutate(., days_before_release = as.numeric(release_date - vdate)) %>%
	as.data.table(.) %>%
	split(., by = c('date', 'varname')) %>%
	lapply(., function(x)
		x %>%
			dcast(., days_before_release ~ forecast, value.var = 'value') %>%
			merge(
				data.table(days_before_release = seq(max(.$days_before_release), 1, -1)),
				.,
				by = 'days_before_release',
				all = TRUE
			) %>%
			.[order(-days_before_release)] %>%
			.[, colnames(.) := lapply(.SD, function(x) zoo::na.locf(x, na.rm = F)), .SDcols = colnames(.)] %>%
			melt(., id.vars = 'days_before_release', value.name = 'value', variable.name = 'forecast', na.rm = T) %>%
			.[,
				c('varname', 'date', 'release_date') :=
					list(x$varname[[1]], x$date[[1]], x$release_date[[1]])
			]
	) %>%
	rbindlist(.) %>%
	# .[, vdate := release_date - days_before_release] %>%
	dcast(., varname + date + release_date + days_before_release ~ forecast, value.var = 'value')


## Train Model ----------------------------------------------------------
train_varnames =
	# unique(hist_data$varname)
	c('ffr', 'sofr')

trees = lapply(train_varnames, function(this_varname) {
	
	input_data =
		train_data %>%
		.[varname == this_varname] #%>%
		# .[!year(date) %in% c(2020)]
		# .[!date %in% as_date(c('2020-04-01', '2020-07-01'))]
	
	tree = xgboost::xgboost(
		data =
			input_data %>%
			transmute(
				.,
				days_before_release = ifelse(days_before_release >= 1000, 1000, days_before_release),
				arima, cb, cbo, constant, fnma, int, ma, now, wsj
			) %>%
			as.matrix(.),
		label = input_data$hist_value,
		verbose = 2,
		max_depth = 5,
		print_every_n = 50,
		nthread = 4,
		nrounds = 200,
		objective = 'reg:squarederror',
		booster = 'gblinear'
		#,
		# monotone_constraints = '(0,1,1,1,1,1,1)'
	)
	
	return(tree)
}) %>%
	set_names(., train_varnames)


## Predict ----------------------------------------------------------
# Predicts values from test data,
# i.e. dates that haven't been forecasted yet at all
test_results = lapply(train_varnames, function(this_varname) {
	message(this_varname)
	
	oos_values =
		predict(
			trees[[this_varname]],
			test_data[varname == this_varname] %>%
				.[, c('days_before_release', 'arima', 'cb', 'cbo', 'constant', 'fnma', 'int', 'ma', 'now', 'wsj')] %>%
				.[, days_before_release := fifelse(days_before_release >= 1000, 1000, days_before_release)] %>%
				as.matrix(.)
		)
	
	predicted_values =
		test_data %>%
		.[varname == this_varname] %>%
		as_tibble(.) %>%
		mutate(., vdate = release_date - days_before_release, predict = oos_values) %>%
		filter(., vdate <= today())
	
	# predicted_values %>%
	# 	# transmute(., varname, date, release_date, days_before_release, vdate, predict) %>%
	# 	filter(., varname %in% c('gdp', 'pce') & date == min(date)) %>%
	# 	pivot_wider(., names_from = varname, values_from = predict) %>%
	# 	View(.)
	return(predicted_values)
}) %>%
	bind_rows(.)

# Show GDP values
test_results %>%
	filter(., varname == 'ffr') %>%
	select(., vdate, date, predict) %>%
	pivot_wider(., names_from = 'date', values_from = 'predict') %>%
	arrange(., desc(vdate)) %>%
	print(., n = 100)

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
