#'  This script runs deprecated submodels
#'  Assumes rate data is not subject to revisions
#'  TBD: Add TD forecasts

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'rates-model-forecast-deprecated-sources'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = F

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log')), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(paste0('Run ', Sys.Date()))
}

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)
library(highcharter)

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
hist = list()
submodels = list()


# Sub-Models ----------------------------------------------------------

## Quandl FFR Futures ----------------------------------------------------------
local({
	
	message('Starting Quandl data scrape')
	quandl_data =
		purrr::map_dfr(1:24, function(j)
			read_csv(
				str_glue('https://www.quandl.com/api/v3/datasets/CHRIS/CME_FF{j}.csv?api_key={CONST$QUANDL_API_KEY}'),
				col_types = 'Ddddddddd'
			) %>%
				transmute(., vdate = Date, settle = Settle, j = j) %>%
				filter(., vdate >= as_date('2010-01-01'))
		) %>%
		transmute(
			.,
			varname = 'ffr',
			vdate,
			date = # Consider the forecasted period the vdate + j
				from_pretty_date(paste0(year(vdate), 'M', month(vdate)), 'm') %>%
				add_with_rollback(., months(j - 1), roll_to_first = TRUE),
			value = 100 - settle
		)
	
	if (RESET_SQL) dbExecute(db, 'DROP TABLE IF EXISTS rates_model_quandl')
	if (!'rates_model_quandl' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(db,
				'CREATE TABLE rates_model_quandl (
				varname VARCHAR(255),
				vdate DATE,
				date DATE,
				value NUMERIC (20, 4),
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT rates_model_quandl_pk PRIMARY KEY (varname, vdate, date)
				)'
		)
		dbExecute(db, 'SELECT create_hypertable(relation => \'rates_model_quandl\', time_column_name => \'vdate\')')
	}
	dbExecute(db, create_insert_query(
		quandl_data,
		'rates_model_quandl',
		'ON CONFLICT (varname, vdate, date) DO UPDATE SET value=EXCLUDED.value'
		))
	
	
	## Now prep and send to SQL
	submodel_data =
		quandl_data %>%
		transmute(., submodel = 'cme_quandl', varname, freq = 'm', vdate, date, value)
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM rates_model_submodel_values')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		submodel_data %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'rates_model_submodel_values',
				'ON CONFLICT (submodel, varname, freq, vdate, date) DO UPDATE SET value=EXCLUDED.value'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	
	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL:')
	print(sum(unlist(sql_result)))
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM rates_model_submodel_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)
	
})
