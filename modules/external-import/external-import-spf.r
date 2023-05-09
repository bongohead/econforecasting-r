# Initialize ----------------------------------------------------------
# If F, adds data to the database that belongs to data vintages already existing in the database.
# Set to T only when there are model updates, new variable pulls, or old vintages are unreliable.
STORE_NEW_ONLY = T
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)
library(readxl, include.only = 'read_excel')

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	# Scrape vintage dates
	vintage_dates =
		request(paste0(
			'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
			'survey-of-professional-forecasters/spf-release-dates.txt?'
		)) %>%
		req_perform %>%
		resp_body_string %>%
		str_sub(., str_locate(., coll('1990 Q2'))[1], str_locate(., coll('*The 1990Q2'))[1] - 1) %>%
		read_csv(., col_names = 'X1', col_types = 'c') %>%
		.$X1 %>%
		map(., function(x)
			x %>%
				str_squish(.) %>%
				str_split(., ' ') %>%
				.[[1]] %>%
				{
					if (length(.) == 4) tibble(X1 = .[1], X2 = .[2], X3 = .[3], X4 = .[4])
					else if (length(.) == 3) tibble(X1 = NA, X2 = .[1], X3 = .[2], X4 = .[3])
					else stop ('Error parsing data')
				}
		) %>%
		list_rbind %>%
		fill(., X1, .direction = 'down') %>%
		transmute(
			.,
			release_date = from_pretty_date(paste0(X1, X2), 'q'),
			vdate = lubridate::mdy(str_replace_all(str_extract(X3, "[^\\s]+"), '[*]', ''))
		) %>%
		# Don't include first date - weirdly has same vintage date as second date
		filter(., release_date >= as_date('2000-01-01'))

	print(arrange(vintage_dates, desc(release_date)))

	# method: sv is for 'single value, refers to level'
	spf_params = tribble(
		~ varname, ~ spfname, ~ transform, ~ single_value,
		't03m', 'TBILL', 'base', F,
		't10y', 'TBOND', 'base', F,
		'gdp', 'RGDP', 'apchg', F,
		'ngdp', 'NGDP', 'apchg', F,
		'pce', 'RCONSUM', 'apchg', F,
		'govts', 'RSLGOV', 'apchg', F,
		'govtf', 'RFEDGOV', 'apchg', F,
		'pdir', 'RRESINV', 'apchg', F,
		'pdin', 'RNRESIN', 'apchg', F,
		'cbi', 'RCBI', 'base', F,
		'cpi', 'CPI', 'base', F,
		'pcepi', 'PCE', 'base', F,
		'unemp', 'UNEMP', 'base', F,
		'houst', 'HOUSING', 'base', F,
		'aaa', 'BOND', 'base', F,
		'baa', 'BAABOND', 'base', F,
		# 'inf5y', 'CPI5YR', 'base', T,
		# 'inf10y', 'CPI10', 'base', T,
		# 'infpce5y', 'PCE5YR', 'base', T,
		# 'infpce10y', 'PCE10', 'base', T
		)


	paste0(
		'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
		'survey-of-professional-forecasters/historical-data/medianlevel.xlsx?la=en'
		) %>%
		request %>%
		req_perform(., path = file.path(tempdir(), paste0('spf.xlsx')))

	spf_data_1_import = map_dfr(df_to_list(spf_params), function(x) {

		message(x$varname)

		read_excel(file.path(tempdir(), paste0('spf.xlsx')), na = '#N/A', sheet = x$spfname) %>%
			select(., c('YEAR', 'QUARTER', {
				if (x$single_value == T && x$transform == 'base') x$spfname
				else if (x$transform == 'apchg') paste0(x$spfname, 1:6)
				else if (x$transform == 'base') paste0(x$spfname, 2:6)
				else stop('Error')
				})) %>%
			mutate(., release_date = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
			select(., -YEAR, -QUARTER) %>%
			pivot_longer(., -release_date, names_to = 'fcPeriods') %>%
			mutate(., fcPeriods = {if (x$single_value == F) as.numeric(str_sub(fcPeriods, -1)) - 2 else 0}) %>%
			mutate(., date = add_with_rollback(release_date, months(fcPeriods * 3))) %>%
			arrange(., date) %>%
			na.omit %>%
			inner_join(., vintage_dates, by = 'release_date') %>%
			transmute(
				.,
				varname = x$varname,
				vdate,
				date,
				value
			)
		})

	# Add in calculated variables
	spf_data_1 = map_dfr(group_split(spf_data_1_import, vdate), function(x)
		x %>%
			pivot_wider(., id_cols = c('vdate', 'date'), names_from = 'varname', values_from = 'value' ) %>%
			arrange(., date) %>%
			mutate(
				.,
				pdi = pdir + pdin,
				govt = govts + govtf
				) %>%
			pivot_longer(., -c('vdate', 'date'), names_to = 'varname', values_to = 'value')
		) %>%
		na.omit %>%
		group_split(., varname, vdate) %>%
		map_dfr(., function(x) {
			transform = x$varname[[1]] %in% c('gdp', 'ngdp', 'pce', 'pdi', 'pdir', 'pdin', 'govts', 'govtf', 'govt')
			x %>% mutate(., value = {if (transform == TRUE) apchg(value, 4) else value})
			}) %>%
		na.omit %>%
		transmute(
			.,
			forecast = 'spf',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
			)


	## Download recession data
	paste0(
		'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
		'survey-of-professional-forecasters/data-files/files/median_recess_level.xlsx?la=en'
		) %>%
		request %>%
		req_perform(., path = file.path(tempdir(), paste0('spf-recession.xlsx')))

	spf_data_2 =
		read_excel(file.path(tempdir(), 'spf-recession.xlsx'), na = '#N/A') %>%
		mutate(., release_date = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
		select(., -YEAR, -QUARTER) %>%
		pivot_longer(., -release_date, names_to = 'fcPeriods') %>%
		mutate(., fcPeriods = as.numeric(str_sub(fcPeriods, -1)) - 1) %>%
		mutate(., date = add_with_rollback(release_date, months(fcPeriods * 3))) %>%
		na.omit %>%
		inner_join(., vintage_dates, by = 'release_date') %>%
		transmute(
			.,
			forecast = 'spf',
			form = 'd1',
			freq = 'q',
			varname = 'recess',
			vdate,
			date,
			value
		)

	spf_data = bind_rows(spf_data_1, spf_data_2)

	raw_data <<- spf_data
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
