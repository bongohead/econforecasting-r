#' Recession Probability Test

# Initialize ----------------------------------------------------------
validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Data --------------------------------------------------------------------

## 10-2 Year Spread --------------------------------------------------------
local({

	input_treasury_data = c(
		'https://home.treasury.gov/system/files/276/yield-curve-rates-2011-2020.csv',
		paste0(
			'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/',
			2021:year(today('US/Eastern')),
			'/all?type=daily_treasury_yield_curve&page&_format=csv'
			)
		) %>%
		map(., .progress = F, \(x) read_csv(x, col_types = 'c')) %>%
		list_rbind %>%
		pivot_longer(., cols = -c('Date'), names_to = 'varname', values_to = 'value') %>%
		separate(., col = 'varname', into = c('ttm_1', 'ttm_2'), sep = ' ') %>%
		mutate(
			.,
			varname = paste0('t', str_pad(ttm_1, 2, pad = '0'), ifelse(ttm_2 == 'Mo', 'm', 'y')),
			date = mdy(Date)
			) %>%
		transmute(., vdate = date, varname, date, value) %>%
		filter(., !is.na(value)) %>%
		arrange(., vdate)

})

#
# ## SPF Survey --------------------------------------------------------
# local({
#
#
#
#
# 	submodels$10_2y_spread <<-
# })
#
#
# ## WSJ Survey --------------------------------------------------------
# local({
#
#
#
#
# 	submodels$10_2y_spread <<-
# })
#
#
