# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-atl-nyf'
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


# Import ------------------------------------------------------------------


## Get GDP Release Dates ---------------------------------------------------
hist_data =
	collect(tbl(db, sql(
		"SELECT val.varname, val.vdate, val.date, val.value
		FROM forecast_hist_values val
		INNER JOIN forecast_variables v ON v.varname = val.varname
		WHERE form = 'd1' AND  v.varname = 'gdp'"
		))) %>%
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

	fred_sources = tribble(
		~ forecast, ~ varname, ~ fred_key,
		'atlnow', 'gdp','GDPNOW',
		'atlnow', 'pce', 'PCENOW',
		'stlnow', 'gdp', 'STLENI'
	)

	fred_data =
		fred_sources %>%
		purrr::transpose(.) %>%
		lapply(., function(x) {
			get_fred_data(x$fred_key, CONST$FRED_API_KEY, .freq = 'q', .return_vintages = T) %>%
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
		bind_rows(.)

	fred_data <<- fred_data
})

## Get NYF Forecasts ---------------------------------------------------
local({

	nyf_url = 'https://www.newyorkfed.org/medialibrary/media/research/policy/nowcast/new-york-fed-staff-nowcast_data_2002-present.xlsx?la=en'
	download.file(nyf_url, file.path(tempdir(), 'nyfnow.xlsx'), mode = 'wb', quiet = TRUE)

	nyf_data =
		readxl::read_excel(file.path(tempdir(), 'nyfnow.xlsx'), sheet = 2, skip = 13) %>%
		rename(., vdate = 1) %>%
		mutate(., vdate = as_date(vdate)) %>%
		pivot_longer(., -vdate, names_to = 'date', values_to = 'value', values_drop_na = T) %>%
		mutate(., date = from_pretty_date(date, 'q')) %>%
		filter(., date >= as_date('2020-01-01')) %>%
		transmute(
			.,
			forecast = 'nyfnow',
			form = 'd1',
			freq = 'q',
			varname = 'gdp',
			vdate,
			date,
			value
			)

	nyf_data <<- nyf_data
})


## Join and Validate ---------------------------------------------------
local({

	raw_data = bind_rows(
		fred_data,
		nyf_data
	)

	raw_data %>%
		group_by(., forecast) %>%
		filter(., vdate == max(vdate)) %>%
		ungroup(.) %>%
		print(.)

	raw_data <<- raw_data
})

## Export SQL Server ------------------------------------------------------------------
local({

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		raw_data %>%
		transmute(., forecast, form, vdate, freq, varname, date, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
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

	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'external-import', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count, last_vdate = max(raw_data$vdate)))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
	) %>%
		dbExecute(db, .)
})

## Finalize ------------------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

