#' Ad-hoc request to get vintage histories for FFR

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
EF_DIR = Sys.getenv('EF_DIR')

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)

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


# Get Data ----------------------------------------------------------

## Get Data ----------------------------------------------------------
data_new = as_tibble(dbGetQuery(db, str_glue(
	"SELECT
		vdate, date, value
	FROM forecast_values
	WHERE forecast = 'int'
		AND freq = 'm'
		AND varname = 'ffr'
		AND form = 'd1'"
	)))

data_old = as_tibble(dbGetQuery(db, str_glue(
	"SELECT
		vdate, date, value
	FROM rates_model_quandl
	WHERE varname = 'ffr'"
)))


data_flat =
	# bind_rows(data_new, data_old) %>%
	data_new %>%
	filter(
		.,
		vdate >= as_date('2020-01-01'),
		date <= as_date('2023-01-01')
		)


## Plot & Export ----------------------------------------------------------
plot =
	data_flat %>%
	filter(., date == as_date('2022-12-01')) %>%
	mutate(., date = as.character(date)) %>%
	ggplot(.) +
	geom_line(aes(x = vdate, y = value))

print(plot)

data_flat %>%
	pivot_wider(., id_cols = 'vdate', names_from = 'date', values_from = 'value') %>%
	View(.)

data_flat %>%
	pivot_wider(., id_cols = 'vdate', names_from = 'date', values_from = 'value') %>%
	arrange(., vdate) %>%
	select(., c('vdate', sort(colnames(.)) %>% .[. != 'vdate'])) %>%
	rename(., vintage_date = vdate) %>%
	write_csv(., file = 'ffr_vintages_20220706.csv')
