# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'model-tracking-comparison-gdp'
EF_DIR = Sys.getenv('EF_DIR')
VARIABLES = c('gdp', 'pce')

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
variables_str = paste0(paste0('\'', VARIABLES, '\''), collapse = ',')

hist_data = collect(tbl(db, sql(str_glue(
		"SELECT val.varname, val.vdate, val.date, val.value
		FROM forecast_hist_values val
		INNER JOIN forecast_variables v ON v.varname = val.varname
		WHERE
			form = 'd1'
			AND v.varname IN ({variables_str})"
	)))) %>%
	group_by(., date) %>%
	{inner_join(
		filter(., vdate == min(vdate)) %>% transmute(., date, varname, first_hist_vdate = vdate, first_hist_value = value),
		filter(., vdate == max(vdate)) %>% transmute(., date, varname, final_hist_vdate = vdate, final_hist_value = value),
		by = c('varname', 'date')
	)} %>%
	ungroup(.) %>%
	arrange(., date)

# Only get forecasts data for when date is < ahead of vdate
forecast_data = collect(tbl(db, sql(str_glue(
	"SELECT val.varname, val.forecast, val.vdate, val.date, val.value
	FROM forecast_values val
	INNER JOIN forecast_variables v ON v.varname = val.varname
	WHERE
		form = 'd1'
		AND date <= vdate + INTERVAL '60 MONTHS' --AND date >= vdate + INTERVAL '1 MONTHS'
		AND v.varname IN ({variables_str})
		AND vdate > '2018-01-01'"
	))))

forecasts = collect(tbl(db, sql('SELECT * FROM forecasts')))


## Compare ---------------------------------------------------
joined_data =
	forecast_data %>%
	inner_join(., hist_data, by = c('date', 'varname')) %>%
	filter(., vdate <= first_hist_vdate) %>%
	mutate(
		.,
		days_before_release = as.numeric(interval(vdate, first_hist_vdate), 'days'),
		time_before_release = case_when(
			days_before_release <= 3 * 30 ~ '0_to_90_days',
			days_before_release <= 12 * 30 ~ '91_to_360_days',
			days_before_release <= 36 * 30 ~ '360_to_1080_days',
			TRUE ~ '1081_days_plus'
		),
		year_vdate = year(vdate),
		error = abs(first_hist_value - value),
		percentage_error = abs(error/first_hist_value)
	) %>%
	inner_join(., forecasts[, c('id', 'fullname')], by = c('forecast' = 'id')) %>%
	mutate(., forecastname = fullname)


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

# selected_varname = 'gdp'



temp_dir = tempdir()
metrics_dir = fs::dir_create(file.path(temp_dir, 'metrics'))
print(metrics_dir)
joined_data %>%
	transmute(
		.,
		varname,
		forecastname,
		forecast_id = forecast,
		obs_date = date,
		forecast_release_date = vdate,
		forecast_value = value,
		realized_value = first_hist_vdate,
		realized_release_date = first_hist_vdate,
		error,
		percentage_error
		) %>%
	arrange(., varname, obs_date) %>%
	mutate(., obs_date = to_pretty_date(obs_date, 'q')) %>%
	write_csv(., file.path(metrics_dir, 'all_forecasts_flat.csv'))


joined_data %>%
	# filter(., varname == selected_varname) %>%
	group_by(., varname, time_before_release) %>%
	group_split(.) %>%
	lapply(., function(x) {

		print(x$time_before_release[[1]])

		df =
			x %>%
			group_by( forecastname, date, first_hist_vdate) %>%
			na.omit(.) %>%
			summarize(
				.,
				MAE = mean(error),
				MAPE = mean(percentage_error),
				n_forecasts = n(),
				.groups = 'drop'
				) %>%
			filter(., n_forecasts >= 1) %>%
			pivot_wider(., id_cols = c(date, first_hist_vdate), names_from = forecastname, values_from = c(MAE, MAPE), names_sep = ' - ') %>%
			arrange(., date) %>%
			mutate(., date = to_pretty_date(date, 'q')) %>%
			rename(., 'Obs Date' = date, 'Release Date' = first_hist_vdate)

		write_csv(df, file.path(metrics_dir, paste0('error_by_obs_quarter_for_', x$varname[[1]], '_forecasts_', x$time_before_release[[1]], '_before_release_date.csv')))

		})



#
# joined_data %>%
# 	filter(., date <= vdate)
# 	group_by(varname, forecast, date) %>%
# 	na.omit(.) %>%
# 	summarize(
# 		.,
# 		mae = mean(error),
# 		mape = mean(percentage_error),
# 		n_forecasts = n(),
# 		.groups = 'drop'
# 	) %>%
# 	filter(., n_forecasts >= 1) %>%
# 	group_split(., varname) %>%
# 	set_names(., map_chr(., ~ .$varname[[1]]))
#
#
# pf_data = performance_data$gdp
#
#
# # All dates
# pf_data %>%
# 	pivot_wider(., id_cols = date, names_from = forecast, values_from = mae) %>%
# 	arrange(., date)
#
#
# pf_data %>%
# 	filter(., date <= vdate + months(1)) %>%
# 	pivot_wider(., id_cols = date, names_from = forecast, values_from = mae) %>%
# 	arrange(., date)


