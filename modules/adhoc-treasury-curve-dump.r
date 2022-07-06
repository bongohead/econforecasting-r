# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
EF_DIR = Sys.getenv('EF_DIR')

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(jsonlite)
library(DBI)
library(RPostgres)
library(econforecasting)

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

# Load Data ----------------------------------------------------------
data = as_tibble(dbGetQuery(db, paste0(
	"SELECT varname, vdate, date, value
	FROM 
	(
		SELECT forecast, freq, form, varname, vdate, date, value, MAX(vdate) OVER (partition by forecast, freq, form, varname) AS max_vdate
		FROM
		(
			SELECT 
				forecast, freq, form, date, varname, MAX(vdate) as vdate, last(value, vdate) as VALUE
			FROM forecast_values
			WHERE 
				varname = ANY('{t01m, t03m, t06m, t01y, t02y, t05y, t10y, t20y, t30y}'::VARCHAR[])
				AND freq = ANY('{m}'::VARCHAR[])
				AND forecast = 'int'
				AND form = 'd1'
			GROUP BY forecast, varname, form, freq, date
			ORDER BY forecast, varname, form, freq, date
		) a 
	) b
	LEFT JOIN forecasts f ON b.forecast = f.id
	WHERE max_vdate = vdate
	"))) %>%
	pivot_wider(., id_cols = c('vdate', 'date'), names_from = 'varname', values_from = 'value') %>%
	transmute(., date_updated = vdate, date_forecasted = date, t01m, t03m, t06m, t01y, t02y, t05y, t10y, t20y, t30y)

data %>% fwrite(., '/var/www/econforecasting.com/public/static/data/treasury_forecasts_all.csv')