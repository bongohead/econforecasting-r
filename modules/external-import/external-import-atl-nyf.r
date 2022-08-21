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
get_fred_data('GDPC1', CONST$FRED_API_KEY, .freq = 'q', .return_vintages = T) %>%
	group_by(., date) %>%
	{inner_join(
		filter(., vintage_date == min(vintage_date)) %>% transmute(., date, first_vdate = vintage_date, first_value = value),
		filter(., vintage_date == max(vintage_date)) %>% transmute(., date, final_vdate = vintage_date, final_value = value),
		by = 'date'
		)} %>%
	ungroup(.)

	# mutate(., position = case_when(
	# 	vintage_date == min(vintage_date) ~ 'first',
	# 	vintage_date == max(vintage_date) ~ 'final',
	# 	TRUE ~ 'intermediate'
	# 	)) %>%
	# ungroup(.) %>%
	# filter(., position %in% c('first', 'final')) %>%
	# pivot_wider(., id_cols = c(date), names_from = position, values_from = c(vintage_date, value)) %>%
	# View(.)
	# #summarize(., first_vdate = min(vintage_date), .groups = 'drop')
	# 






