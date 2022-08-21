# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'model-tracking-comparison-gdp'
EF_DIR = Sys.getenv('EF_DIR')

## Log Job ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(jsonlite)
library(httr)
library(rvest)
library(DBI)
library(RPostgres)
library(econforecasting)
library(lubridate)

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


# Analysis ------------------------------------------------------------------

## Get GDP Release Dates ---------------------------------------------------
hist_data = collect(tbl(db, sql(
		"SELECT val.varname, val.vdate, val.date, val.value
		FROM forecast_hist_values val
		INNER JOIN forecast_variables v ON v.varname = val.varname
		WHERE
			form = 'd1'
			AND v.varname = 'gdp'"
	))) %>%
	group_by(., date) %>%
	{inner_join(
		filter(., vdate == min(vdate)) %>% transmute(., date, varname, first_hist_vdate = vdate, first_hist_value = value),
		filter(., vdate == max(vdate)) %>% transmute(., date, varname, final_hist_vdate = vdate, final_hist_value = value),
		by = c('varname', 'date')
	)} %>%
	ungroup(.) %>%
	arrange(., date)

# Only get forecasts data for when date is < ahead of vdate
forecast_data = collect(tbl(db, sql(
	"SELECT val.varname, val.forecast, val.vdate, val.date, val.value
	FROM forecast_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE
		form = 'd1'
		AND date <= vdate + INTERVAL '24 MONTHS' --AND date >= vdate + INTERVAL '1 MONTHS'
		AND v.varname = 'gdp'
		AND vdate > '2018-01-01'"
	)))


## Compare ---------------------------------------------------
joined_data =
	forecast_data %>%
	inner_join(., hist_data, by = c('date', 'varname')) %>%
	filter(., vdate <= first_hist_vdate) %>%
	mutate(
		.,
		year_vdate = year(vdate),
		error = abs(first_hist_value - value),
		percentage_error = abs(error/first_hist_value)
	)


# performance_data =
# 	joined_data %>%
# 	group_by(varname, forecast, vdate) %>%
# 	na.omit(.) %>%
# 	summarize(., mae = mean(error), n_forecasts = n(), .groups = 'drop') %>%
# 	filter(., n_forecasts >= 4) %>%
# 	group_split(., varname) %>%
# 	set_names(., map_chr(., ~ .$varname[[1]])) %>%
# 	lapply(., function(x)
# 		x %>%
# 			pivot_wider(., id_cols = forecast, names_from = vdate, values_from = mae)
# 	)

selected_varname = 'gdp'

joined_data %>%
	filter(., varname == selected_varname) %>%
	group_by( forecast, date) %>%
	na.omit(.) %>%
	summarize(
		.,
		mae = mean(error),
		mape = mean(percentage_error),
		n_forecasts = n(),
		.groups = 'drop'
		) %>%
	filter(., n_forecasts >= 1) %>%
	pivot_wider(., id_cols = date, names_from = forecast, values_from = mae) %>%
	arrange(., date)

joined_data %>%
	filter(., date <= vdate)
	group_by(varname, forecast, date) %>%
	na.omit(.) %>%
	summarize(
		.,
		mae = mean(error),
		mape = mean(percentage_error),
		n_forecasts = n(),
		.groups = 'drop'
	) %>%
	filter(., n_forecasts >= 1) %>%
	group_split(., varname) %>%
	set_names(., map_chr(., ~ .$varname[[1]]))


pf_data = performance_data$gdp


# All dates
pf_data %>%
	pivot_wider(., id_cols = date, names_from = forecast, values_from = mae) %>%
	arrange(., date)


pf_data %>%
	filter(., date <= vdate + months(1)) %>%
	pivot_wider(., id_cols = date, names_from = forecast, values_from = mae) %>%
	arrange(., date)


