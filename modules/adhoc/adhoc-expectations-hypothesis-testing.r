#' Expectations hypothesis model testing
#'
#' Expectations theory hypothesizes that:
#'  t06m(t=0) = t(03m, t=0) * t(03m, t=3m) + reinvestment_risk_premium(t03m, t=3m)
#'


## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

## Get Data ----------------------------------------------------------------
spf_data = get_query(pg,
	"SELECT vdate, date, value FROM forecast_values_v2 WHERE varname = 't03m' AND freq = 'q' AND form = 'd1' AND forecast = 'spf'"
	)

hist_data = get_query(pg,
	"SELECT vdate, date, value FROM forecast_hist_values_v2 WHERE varname IN ('t03m', 't06m') AND form = 'd1' AND freq = 'm'"
	)


# Analysis ----------------------------------------------------------------

# Survey date closes 3 days before, but let's approximate it to the start of the month
spf_data %>%
	mutate(., survey_date = floor_date(vdate, 'months'))
