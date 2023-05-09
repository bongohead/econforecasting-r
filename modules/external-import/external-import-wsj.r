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
library(readxl, include.only = c('excel_sheets', 'read_excel'))

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Import ------------------------------------------------------------------

## Get Data ------------------------------------------------------------------
local({

	message('**** WSJ Survey')
	message('***** Last Import Apr 23')

	wsj_params = tribble(
		~ submodel, ~ fullname,
		'wsj', 'WSJ Consensus',
		# 'wsj_wfc', 'Wells Fargo & Co.',
		# 'wsj_gsu', 'Georgia State University',
		# 'wsj_sp', 'S&P Global Ratings',
		# 'wsj_ucla', 'UCLA Anderson Forecast',
		# 'wsj_gs', 'Goldman, Sachs & Co.',
		# 'wsj_ms', 'Morgan Stanley'
	)

	xl_path = file.path(Sys.getenv('EF_DIR'), 'modules', 'external-import', 'external-import-wsj.xlsx')

	wsj_data_0 = map(keep(excel_sheets(xl_path), \(x) str_extract(x, '[^_]+') == 'wsj'), function(sheetname) {

		vdate = str_extract(sheetname, '(?<=_).*')

		col_names = suppressMessages(read_excel(
			xl_path,
			sheet = sheetname,
			col_names = F,
			.name_repair = 'universal',
			n_max = 2
			)) %>%
			replace(., is.na(.), '') %>%
			{paste0(.[1,], '-', .[2,])}

		if (length(unique(col_names)) != length(col_names)) stop('WSJ Input Error!')

		read_excel(
			xl_path,
			sheet = sheetname,
			col_names = col_names,
			skip = 2
		) %>%
			pivot_longer(
				cols = -'-Forecast',
				names_to = 'varname_date',
				values_to = 'value'
			) %>%
			transmute(
				vdate = as_date(vdate),
				fullname = .$'-Forecast',
				varname = str_extract(varname_date, '[^-]+'),
				date = from_pretty_date(str_extract(varname_date, '(?<=-).*'), 'q'),
				value
			) %>%
			inner_join(., wsj_params, by = 'fullname') %>%
			transmute(., submodel, varname, freq = 'q', vdate, date, value)
		}) %>%
		list_rbind %>%
		na.omit

	wsj_data = imap(group_split(wsj_data_0, submodel, varname, freq, vdate), function(z, i) {
		tibble(
			date = seq(from = min(z$date, na.rm = T), to = max(z$date, na.rm = T), by = '3 months')
			) %>%
			left_join(., z, by = 'date') %>%
			mutate(., value = zoo::na.approx(value)) %>%
			transmute(
				.,
				submodel = unique(z$submodel),
				varname = unique(z$varname),
				freq = unique(z$freq),
				vdate = unique(z$vdate),
				date,
				value
			)
		}) %>%
		list_rbind %>%
		transmute(
			.,
			forecast = 'wsj',
			form = 'd1',
			freq = 'q',
			varname,
			vdate,
			date,
			value
		)

	raw_data <<- wsj_data
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
