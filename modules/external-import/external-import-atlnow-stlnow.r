# Initialize ----------------------------------------------------------
# If F, adds data to the database that belongs to data vintages already existing in the database.
# Set to T only when there are model updates, new variable pulls, or old vintages are unreliable.
STORE_NEW_ONLY = T
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)

## Load Env & Logging ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get GDP Release Dates ---------------------------------------------------
hist_data =
	get_query(pg, sql(
		"SELECT val.varname, val.vdate, val.date, val.value
		FROM forecast_hist_values_v2 val
		INNER JOIN forecast_variables v ON v.varname = val.varname
		WHERE form = 'd1' AND  v.varname = 'gdp'"
		)) %>%
	group_by(., date) %>%
	{inner_join(
		filter(., vdate == min(vdate)) %>% transmute(., date, first_vdate = vdate, first_value = value),
		filter(., vdate == max(vdate)) %>% transmute(., date, final_vdate = vdate, final_value = value),
		by = 'date'
		)} %>%
	ungroup(.) %>%
	arrange(., date)

## Get ATL + STL Forecasts ---------------------------------------------------
local({

	api_key = Sys.getenv('FRED_API_KEY')

	fred_sources = tribble(
		~ forecast, ~ varname, ~ fred_key,
		'atlnow', 'gdp','GDPNOW',
		'atlnow', 'pce', 'PCENOW',
		'stlnow', 'gdp', 'STLENI'
	)

	fred_data = lapply(df_to_list(fred_sources), function(x) {
		get_fred_data(x$fred_key, api_key, .freq = 'q', .return_vintages = T) %>%
			filter(., date >= as_date('2020-01-01')) %>%
			transmute(
				.,
				forecast = x$forecast,
				form = 'd1',
				freq = 'q',
				varname = x$varname,
				vdate = vintage_date,
				date,
				value
				)
		}) %>%
		list_rbind(.)

	fred_data <<- fred_data
})

# ## Get NYF Forecasts ---------------------------------------------------
# local({
#
# 	nyf_url = 'https://www.newyorkfed.org/medialibrary/media/research/policy/nowcast/new-york-fed-staff-nowcast_data_2002-present.xlsx?la=en'
# 	download.file(nyf_url, file.path(tempdir(), 'nyfnow.xlsx'), mode = 'wb', quiet = TRUE)
#
# 	nyf_data =
# 		readxl::read_excel(file.path(tempdir(), 'nyfnow.xlsx'), sheet = 2, skip = 13) %>%
# 		rename(., vdate = 1) %>%
# 		mutate(., vdate = as_date(vdate)) %>%
# 		pivot_longer(., -vdate, names_to = 'date', values_to = 'value', values_drop_na = T) %>%
# 		mutate(., date = from_pretty_date(date, 'q')) %>%
# 		filter(., date >= as_date('2020-01-01')) %>%
# 		transmute(
# 			.,
# 			forecast = 'nyfnow',
# 			form = 'd1',
# 			freq = 'q',
# 			varname = 'gdp',
# 			vdate,
# 			date,
# 			value
# 			)
#
# 	nyf_data <<- nyf_data
# })


## Join and Validate ---------------------------------------------------
local({

	raw_data = bind_rows(
		fred_data
		# nyf_data
	)

	raw_data %>%
		group_by(., forecast) %>%
		filter(., vdate == max(vdate)) %>%
		ungroup(.) %>%
		print(.)

	raw_data <<- raw_data
})

## Export Forecasts ------------------------------------------------------------------
local({

	# Store in SQL
	model_values = transmute(raw_data, forecast, form, vdate, freq, varname, date, value)

	rows_added = store_forecast_values_v2(pg, model_values, .store_new_only = STORE_NEW_ONLY, .verbose = T)

	# Log
	validation_log$store_new_only <<- STORE_NEW_ONLY
	validation_log$rows_added <<- rows_added
	validation_log$last_vdate <<- max(raw_data$vdate)

	disconnect_db(pg)
})

