# Initialize ----------------------------------------------------------
# If F, adds data to the database that belongs to data vintages already existing in the database.
# Set to T only when there are model updates, new variable pulls, or old vintages are unreliable.
STORE_NEW_ONLY = T
validation_log <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(readxl, include.only = c('excel_sheets', 'read_excel'))
library(fs, include.only = c('file_temp'))

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Import ------------------------------------------------------------------

## Set Search Params ------------------------------------------------------------------
local({

	orgs_to_retain = tribble(
		~ submodel, ~ org,
		'wsj', 'WSJ Consensus',
		# 'wsj_wfc', 'Wells Fargo & Co.',
		# 'wsj_gsu', 'Georgia State University',
		# 'wsj_sp', 'S&P Global Ratings',
		# 'wsj_ucla', 'UCLA Anderson Forecast',
		# 'wsj_gs', 'Goldman, Sachs & Co.',
		# 'wsj_ms', 'Morgan Stanley'
	)

	wsj_search = tribble(
		~ vdate, ~ filename,
		'2022-07-17', 'https://s.wsj.net/public/resources/documents/wsjecon0722.xlsx',
		'2022-10-16', 'https://s.wsj.net/public/resources/documents/wsjecon1022.xlsx',
		'2023-01-15', 'https://s.wsj.net/public/resources/documents/wsjecon0123.xlsx',
		'2023-04-23', 'https://s.wsj.net/public/resources/documents/wsjecon0423.xlsx',
		'2023-07-15', 'https://s.wsj.net/public/resources/documents/wsjecon0723.xlsx',
		'2023-10-26', 'https://s.wsj.net/public/resources/documents/wsjecon1023.xlsx'
	)

	orgs_to_retain <<- orgs_to_retain
	wsj_search <<- wsj_search
})

## Get First Round of Data ------------------------------------------------------------------
local({

	xl_path = file.path(Sys.getenv('EF_DIR'), 'modules', 'external-import', 'external-import-wsj.xlsx')

	old_data = map(keep(excel_sheets(xl_path), \(x) str_extract(x, '[^_]+') == 'wsj'), function(sheetname) {

		vdate = str_extract(sheetname, '(?<=_).*')

		col_names =
			suppressMessages(read_excel(xl_path,  sheet = sheetname, col_names = F, .name_repair = 'universal', n_max = 2)) %>%
			replace(., is.na(.), '') %>%
			{paste0(.[1,], '-', .[2,])}

		if (length(unique(col_names)) != length(col_names)) stop('WSJ Input Error!')

		read_excel(xl_path, sheet = sheetname, col_names = col_names, skip = 2) %>%
			pivot_longer(cols = -'-Forecast', names_to = 'varname_date', values_to = 'value') %>%
			transmute(
				vdate = as_date(vdate),
				org = .$'-Forecast',
				varname = str_extract(varname_date, '[^-]+'),
				date = from_pretty_date(str_extract(varname_date, '(?<=-).*'), 'q'),
				value
			) %>%
			transmute(., org, varname, vdate, date, value)
		}) %>%
		list_rbind %>%
		na.omit


	old_data <<- old_data
})

## Pull Recent Data ------------------------------------------------------------------
local({

	pulled_data = list_rbind(map(df_to_list(wsj_search), .progress = T, function(search_params) {

		filename = file_temp(ext = '.xlsx')

		request(search_params$filename) %>%
			add_standard_headers() %>%
			req_perform() %>%
			resp_body_raw() %>%
			writeBin(., filename)

		# Get the varname-x-date headers from the first two rows of the spreadsheet
		cols =
			suppressMessages(read_excel(filename, sheet = 1, col_names = F, col_types = 'text', n_max = 2)) %>%
			select(3:ncol(.)) %>%
			set_names(., paste0('c', 1:ncol(.) + 2)) %>%
			bind_cols(rn = c('r1', 'r2'), .) %>%
			pivot_longer(., cols = -rn, names_transform = \(x) as.integer(str_replace(x, 'c', '')), names_to = 'cn') %>%
			pivot_wider(., id_cols = 'cn', names_from = 'rn') %>%
			arrange(., cn) %>%
			fill(., r1)

		# Extract data, pivot it longer and join by the column index
		imported_data =
			suppressMessages(read_excel(filename, sheet = 1, col_names = F, col_types = 'text', skip = 2)) %>%
			set_names(., paste0('c', 1:ncol(.))) %>%
			mutate(., rn = 1:nrow(.))

		# Get row numbers corresponding to orgs & wsj_consensus
		org_rns = imported_data %>% select(., rn, c1, c2) %>% filter(., !is.na(c1) & !is.na(c2)) %>% .$rn
		wsj_rn = imported_data %>% select(., rn, c1, c2) %>% filter(., is.na(c1) & !is.na(c2)) %>% head(., 1) %>% .$rn

		pivoted_data =
			imported_data %>%
			pivot_longer(., cols = -c(rn, c2), names_transform = \(x) as.integer(str_replace(x, 'c', '')), names_to = 'cn') %>%
			rename(., org = c2) %>%
			mutate(., org = ifelse(rn == wsj_rn, 'WSJ Consensus', org)) %>%
			filter(., rn <= wsj_rn)

		cleaned_data =
			pivoted_data %>%
			filter(., cn >= 3) %>%
			inner_join(., cols, by = c('cn'), relationship = 'many-to-one') %>%
			mutate(
				.,
				r1 = str_to_lower(r1),
				r2 = str_to_lower(str_replace_all(r2, c('\\.' = '', 'On ' = ''))),
				) %>%
			filter(
				.,
				!str_detect(r1, 'longer-run|payrolls|pce|personal consumption expenditures|4q/4q'),
				!str_detect(r2, 'full year')
				) %>%
			mutate(., varname = case_when(
				str_detect(r1, '10-year treasury') ~ 't10y',
				str_detect(r1, 'federal funds') ~ 'ffr',
				str_detect(r1, 'gdp') ~ 'gdp',
				str_detect(r1, 'recession') ~ 'recess',
				str_detect(r1, 'cpi') ~ 'cpi',
				str_detect(r1, 'unemp') ~ 'unemp',
				TRUE ~ NA_character_
			)) %>%
			mutate(., date = floor_date(case_when(
				varname == 'recess' ~ as_date(search_params$vdate),
				str_detect(r2, '(june|dec) \\d{4}') ~ as_date(fast_strptime(r2, '%b %Y')),
				str_detect(r2, '(june|dec) \\d{2}, \\d{4}') ~ as_date(fast_strptime(r2, '%b %d, %Y')),
				str_detect(r2, '(first|second|third|fourth) quarter \\d{4}') ~ as_date(fast_strptime(str_replace_all(
					r2,
					c('first quarter ' = '01/01/', 'second quarter ' = '04/01/', 'third quarter ' = '07/01/', 'fourth quarter ' = '10/01/'),
					), '%m/%d/%Y')),
				TRUE ~ NA_Date_
			), 'quarter')) %>%
			select(., org, varname, date, value) %>%
			filter(., !is.na(value) & str_squish(value) != '' & value != '-') %>%
			mutate(., value = as.numeric(value)) %>%
			mutate(., vdate = as_date(search_params$vdate))

		if (summarize(cleaned_data, sum(is.na(org)))[[1]] != 0) stop('Missing org name')
		if (summarize(cleaned_data, sum(is.na(varname)))[[1]] != 0) stop('Extra variables not filtered or converted to valid varname')
		if (summarize(cleaned_data, sum(is.na(date)))[[1]] != 0) stop('Extra dates not filtered or converted to valid date')

		return(cleaned_data)
		}))


	# Check disparity between variables
	pulled_data %>%
		filter(., org == 'WSJ') %>%
		group_by(., vdate, varname) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(., names_from = varname, id_cols = vdate, values_from = n) %>%
		arrange(., desc(vdate))

	pulled_data <<- pulled_data
})

## Join Data ------------------------------------------------------------------
local({

	## Add Old Recession Data
	# Pre-4/2021 recession data, created by wsj-backfill-raw-html
	old_recess_data = read_csv(
		file.path(Sys.getenv('EF_DIR'), 'modules', 'external-import', 'wsj-backfill-cleaned-recess.csv'),
		col_types = 'ccccDDd'
	)

	joined_data =
		bind_rows(old_data, pulled_data, old_recess_data) %>%
		# Inner join WSJ params
		inner_join(., orgs_to_retain, by = 'org') %>%
		group_split(., org, varname, vdate) %>%
		imap(., function(z, i)
			tibble(date = seq(from = min(z$date, na.rm = T), to = max(z$date, na.rm = T), by = '3 months')) %>%
				left_join(., z, by = 'date') %>%
				mutate(., value = zoo::na.approx(value)) %>%
				transmute(
					.,
					org = unique(z$org),
					varname = unique(z$varname),
					vdate = unique(z$vdate),
					date,
					value
				)
		) %>%
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

	raw_data = bind_rows(joined_data, old_recess_data)


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
