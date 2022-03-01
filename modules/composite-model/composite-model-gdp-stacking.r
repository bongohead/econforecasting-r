#' This gets historical data for website

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'composite-model-stacking'
EF_DIR = Sys.getenv('EF_DIR')
BACKTEST_DATE_START = '2010-01-01'

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
	WHERE release = 'BEA.GDP' AND freq = 'q' AND form = 'd1' AND v.varname NOT IN ('cbi', 'net_exports')"
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
	WHERE release = 'BEA.GDP' AND freq = 'q' AND form = 'd1' AND v.varname NOT IN ('cbi', 'net_exports')"
	)) %>%
	collect(.)

## Import Forecasts ----------------------------------------------------------
release_data = tbl(db, sql(str_glue(
	"SELECT v.varname, d.release, d.date AS release_date
	FROM forecast_hist_release_dates d
	INNER JOIN forecast_hist_releases r ON r.id = d.release
	INNER JOIN forecast_variables v ON r.id = v.release
	WHERE v.release = 'BEA.GDP'"
	))) %>%
	collect(.) %>%
	# Convert BEA forecasts into release dates
	mutate(., date = add_with_rollback(floor_date(release_date, 'quarter'), months(-3))) %>%
	group_by(., date) %>%
	summarize(., release_date = min(release_date)) %>%
	# Further release dates by guessing
	# Necessary to generate forecasts later on
	bind_rows(
		.,
		tibble(
			date = seq(max(.$date), by = '3 months', length.out = 21)[2:21],
			release_date = seq(max(.$release_date), by = '3 months', length.out = 21)[2:21],
			)
		)

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

## ARIMA Forecasts ----------------------------------------------------------
arima_forecasts =
	model_data %>%
	split(., by = c('varname', 'vdate')) %>%
	keep(., ~ nrow(.) >= 5) %>%
	lapply(., function(x) {
		input_df =
			x[order(date)] %>%
			.[, value.l1 := shift(value, 1, type = 'lag')] %>%
			.[, constant := 1] %>%
			.[2:nrow(.)]

		x_mat = as.matrix(input_df[, c('value.l1', 'constant')])

		y_mat = as.matrix(input_df[, 'value'])

		coef_mat = solve(t(x_mat) %*% x_mat) %*% t(x_mat) %*% y_mat

		## 20 period forecasts
		data.table(
			varname = x$varname[[1]],
			vdate = x$vdate[[1]],
			date = seq(tail(input_df$date, 1), length.out = 21, by = '1 quarter')[2:21],
			value = accumulate(1:20, function(accum, x)
				accum * coef_mat[[1, 1]] + coef_mat[[2, 1]],
				.init = tail(input_df$value, 1)
				)[2:21]
			)
		}) %>%
	rbindlist(.) %>%
	.[, forecast := 'arima']

forecast_data_final = bind_rows(forecast_data, arima_forecasts)

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
	filter(., date >= '2010-01-01') %>%
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
train_varnames = unique(hist_data$varname) #c('pdir', 'pdin', 'pdi', 'gdp', 'pce')

trees = lapply(train_varnames, function(this_varname) {

	input_data =
		train_data %>%
		.[varname == this_varname] %>%
		.[year(date) != '2020']

	tree = xgboost::xgboost(
		data =
			input_data %>%
			transmute(
				.,
				days_before_release,
				arima, cbo, fnma, now, spf, wsj
			) %>%
			as.matrix(.),
		label = input_data$hist_value,
		verbose = 2,
		max.depth = 10,
		nthread = 4,
		nrounds = 200,
		objective = 'reg:squarederror'
		)

	return(tree)
	}) %>%
	set_names(., train_varnames)


## Predict ----------------------------------------------------------
# Predicts values from test data,
# i.e. dates that haven't been forecasted yet at all
test_results = lapply(train_varnames, function(this_varname) {

	oos_values =
		predict(
			trees[[this_varname]],
			test_data[varname == this_varname] %>%
				.[, c('days_before_release', 'arima', 'cbo', 'fnma', 'now', 'spf', 'wsj')] %>%
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
	filter(., varname == 'gdp') %>%
	select(., vdate, date, predict) %>%
	pivot_wider(., names_from = 'date', values_from = 'predict') %>%
	arrange(., desc(vdate)) %>%
	print(., n = 100)


## SQL ----------------------------------------------------------




