#'  Run this script on scheduler after close of business each day

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(DIR, 'logs', 'pull-data.log'), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(paste0('Run ', Sys.Date()))
}

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)

## Load Connection Info ----------------------------------------------------------
source(file.path(DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
	)
releases = list()
hist = list()
ext = list()

## Load Variable Defs ----------------------------------------------------------'
variable_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'variables')
release_params = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'data-releases')

# Release Data ----------------------------------------------------------

## 1. Get Data Releases ----------------------------------------------------------
local({

	message('***** Getting Releases History')

	fred_releases =
		variable_params %>%
		group_by(., relkey) %>%
		summarize(., n_varnames = n(), varnames = jsonlite::toJSON(fullname), .groups = 'drop') %>%
		left_join(
			.,
			variable_params  %>%
				filter(., nc_dfm_input == 1) %>%
				group_by(., relkey) %>%
				summarize(., n_dfm_varnames = n(), dfm_varnames = jsonlite::toJSON(fullname), .groups = 'drop'),
			variable_params %>%
				group_by(., relkey) %>%
				summarize(., n_varnames = n(), varnames = jsonlite::toJSON(fullname), .groups = 'drop'),
			by = 'relkey'
		) %>%
		left_join(., release_params, by = 'relkey') %>%
		# Now create a column of included releaseDates
		left_join(
			.,
			purrr::map_dfr(purrr::transpose(filter(., relsc == 'fred')), function(x) {

				httr::RETRY(
					'GET',
					str_glue(
						'https://api.stlouisfed.org/fred/release/dates?',
						'release_id={x$relsckey}&realtime_start=2020-01-01',
						'&include_release_dates_with_no_data=true&api_key={CONST$FRED_API_KEY}&file_type=json'
					),
					times = 10
					) %>%
					httr::content(., as = 'parsed') %>%
					.$release_dates %>%
					sapply(., function(y) y$date) %>%
					tibble(relkey = x$relkey, reldates = .)
				}) %>%
				group_by(., relkey) %>%
				summarize(., reldates = jsonlite::toJSON(reldates), .groups = 'drop'),
			by = 'relkey'
			)

	releases$fred <<- fred_releases
})

## 2. Combine ----------------------------------------------------------
local({

	releases_final <<- bind_rows(releases)
})

# Historical Data ----------------------------------------------------------

## 1. FRED ----------------------------------------------------------
local({

	message('***** Importing FRED Data')

	fred_data =
		variable_params %>%
		purrr::transpose(.)	%>%
		purrr::keep(., ~ .$source == 'fred') %>%
		purrr::imap_dfr(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_data(
					x$sckey,
					CONST$FRED_API_KEY,
					.freq = x$freq,
					.return_vintages = TRUE,
					.verbose = F
					) %>%
				transmute(
					.,
					sourcename = 'fred',
					varname = x$varname,
					transform = 'base',
					freq = x$freq,
					date,
					vdate = vintage_date,
					value
					) %>%
				filter(
					.,
					date >= as_date('2010-01-01'),
					vdate >= as_date('2010-01-01')
					)
			})

	hist$fred <<- fred_data
})

## 2. Yahoo Finance ----------------------------------------------------------
local({

	message('***** Importing Yahoo Finance Data')

	yahoo_data =
		variable_params %>%
		purrr::transpose(.) %>%
		purrr::keep(., ~ .$source == 'yahoo') %>%
		purrr::map_dfr(., function(x) {
			url =
				paste0(
					'https://query1.finance.yahoo.com/v7/finance/download/', x$sckey,
					'?period1=', '1262304000', # 12/30/1999
					'&period2=', as.numeric(as.POSIXct(Sys.Date() + lubridate::days(1))),
					'&interval=1d',
					'&events=history&includeAdjustedClose=true'
				)
			data.table::fread(url, showProgress = FALSE) %>%
				.[, c('Date', 'Adj Close')]	%>%
				set_names(., c('date', 'value')) %>%
				as_tibble(.) %>%
				# Bug with yahoo finance returning null for date 7/22/21 as of 7/23
				filter(., value != 'null') %>%
				mutate(
					.,
					sourcename = 'yahoo',
					varname = x$varname,
					transform = 'base',
					freq = x$freq,
					date,
					vdate = date,
					value = as.numeric(value)
					) %>%
				return(.)
		})

	hist$yahoo <<- yahoo_data
})

## 3. Calculated Variables ----------------------------------------------------------
local({

	message('***** Adding Calculated Variables')

	fred_data =
		variable_params %>%
		filter(., str_detect(fullname, 'Treasury Yield') | varname == 'ffr') %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(x) {
			get_fred_data(x$sckey, CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
				transmute(., varname = x$varname, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date('2010-01-01'))
		})

	# Monthly aggregation & append EOM with current val
	fred_data_cat =
		fred_data %>%
		group_split(., varname) %>%
		# Add monthly values for current month
		map_dfr(., function(x)
			x %>%
				mutate(., date = as.Date(paste0(year(date), '-', month(date), '-01'))) %>%
				group_by(., varname, date) %>%
				summarize(., value = mean(value), .groups = 'drop') %>%
				mutate(., freq = 'm')
		)

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		variable_params %>%
		purrr::transpose(.) %>%
		map_chr(., ~.$varname) %>%
		unique(.) %>%
		purrr::keep(., ~ str_sub(., 1, 1) == 't' & str_length(.) == 4) %>%
		tibble(varname = .) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))

	# Create training dataset from SPREAD from ffr - fitted on last 3 months
	train_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		select(., -freq) %>%
		filter(., date >= add_with_rollback(Sys.Date(), months(-3))) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(., transmute(filter(fred_data_cat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	#' Calculate DNS fit
	#'
	#' @param df: (tibble) A tibble continuing columns obsDate, value, and ttm
	#' @param return_all: (boolean) FALSE by default.
	#' If FALSE, will return only the MAPE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			mutate(
				.,
				f1 = 1,
				f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm),
				f3 = f2 - exp(-1 * lambda * ttm)
			) %>%
			group_by(date) %>%
			group_split(.) %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				bind_cols(x, fitted = fitted(reg)) %>%
					mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
					mutate(., resid = value - fitted)
			}) %>%
			bind_rows(.) %>%
			{
				if (return_all == FALSE) summarise(., mse = mean(abs(resid))) %>% .$mse
				else .
			} %>%
			return(.)
	}

	# Find MSE-minimizing lambda value
	optim_lambda =
		optimize(
			get_dns_fit,
			df = train_df,
			return_all = FALSE,
			interval = c(-1, 1),
			maximum = FALSE
		)$minimum

	hist_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		select(., -freq) %>%
		filter(., date >= add_with_rollback(Sys.Date(), months(-120))) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(., transmute(filter(fred_data_cat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	# Store DNS coefficients
	dns_data =
		get_dns_fit(df = hist_df, optim_lambda, return_all = TRUE) %>%
		group_by(., date) %>%
		summarize(., tdns1 = unique(b1), tdns2 = unique(b2), tdns3 = unique(b3)) %>%
		pivot_longer(., -date, names_to = 'varname', values_to = 'value') %>%
		transmute(
			.,
			sourcename = 'calc',
			varname,
			transform = 'base',
			freq = 'm',
			date,
			vdate =
				# Avoid storing too much old DNS data - assign vdate as newesty possible value of edit for
				# previous months
				as_date(ifelse(ceiling_date(date, 'months') <= today(), ceiling_date(date, 'months'), today())),
			value = as.numeric(value)
		)


	# For each vintage date, pull all CPI values from the latest available vintage for each date
	# This intelligently handles revisions and prevents
	# duplications of CPI values by vintage dates/obs date
	cpi_df =
		hist$fred %>%
		filter(., varname == 'cpi' & transform == 'base' & freq == 'm') %>%
		select(., date, vdate, value)

	inf_data =
		cpi_df %>%
		group_split(., vdate) %>%
		purrr::map_dfr(., function(x)
			filter(cpi_df, vdate <= x$vdate[[1]]) %>%
				group_by(., date) %>%
				filter(., vdate == max(vdate)) %>%
				ungroup(.) %>%
				arrange(., date) %>%
				transmute(date, vdate = roll::roll_max(vdate, 13), value = 100 * (value/lag(value, 12) - 1))
			) %>%
		na.omit(.) %>%
		distinct(., date, vdate, value) %>%
		transmute(
			.,
			sourcename = 'calc',
			varname = 'inf',
			transform = 'base',
			freq = 'm',
			date,
			vdate,
			value
		)


	# Calculate misc interest rate spreads from cross-weekly/daily rates
	# Assumes no revisions for this data
	rates_wd_data = tribble(
		~ varname, ~ var_w, ~ var_d,
		'mort30yt30yspread', 'mort30y', 't30y',
		'mort15yt10yspread', 'mort15y', 't10y'
		) %>%
		purrr::transpose(., .names = .$varname) %>%
		purrr::map_dfr(., function(x) {

			# Use weekly data as mortgage pulls are weekly
			var_w_df =
				filter(hist$fred, varname == x$var_w & transform == 'base' & freq == 'w') %>%
				select(., varname, date, vdate, value)

			var_d_df =
				filter(hist$fred, varname == x$var_d & transform == 'base' & freq == 'd') %>%
				select(., varname, date, vdate, value)

			# Get week-end days
			w_obs_dates =
				filter(hist$fred, varname == x$var_w & transform == 'base' & freq == 'w') %>%
				.$date %>%
				unique(.)

			# Iterate through the week-end days
			purrr::map_dfr(w_obs_dates, function(w_obs_date) {

				var_d_df_x =
					var_d_df %>%
					filter(., date %in% seq(w_obs_date - days(6), to = w_obs_date, by = '1 day')) %>%
					summarize(., d_vdate = max(vdate), d_value = mean(value))

				var_w_df_x =
					var_w_df %>%
					filter(., date == w_obs_date) %>%
					transmute(., w_vdate = vdate, w_value = value)

				if (nrow(var_d_df_x) == 0 | nrow(var_w_df_x) == 0) return(NA)

				bind_cols(var_w_df_x, var_d_df_x) %>%
					transmute(
						.,
						sourcename = 'calc',
						varname = x$varname,
						transform = 'base',
						freq = 'w',
						date = w_obs_date,
						vdate = max(w_vdate, d_vdate),
						value = w_value - d_value
						) %>%
					return(.)

				}) %>%
				distinct(.)
			})


	hist_calc = bind_rows(dns_data, inf_data, rates_wd_data)

	hist$calc <<- hist_calc
})


## 4. Aggregate Frequencies ----------------------------------------------------------
local({

	hist_agg_0 = bind_rows(hist)

	monthly_agg =
		# Get all daily/weekly varnames with pre-existing data
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w')) %>%
		# Add monthly values for current month
		mutate(
			.,
			freq = 'm',
			date = lubridate::floor_date(date, 'month')
			) %>%
		group_by(., sourcename, varname, transform, freq, date, vdate) %>%
		summarize(., value = mean(value), .groups = 'drop')

	quarterly_agg =
		hist_agg_0 %>%
		filter(., freq %in% c('d', 'w', 'm')) %>%
		mutate(
			.,
			freq = 'q',
			date = lubridate::floor_date(date, 'quarter')
			) %>%
		group_by(., sourcename, varname, transform, freq, date, vdate) %>%
		summarize(., value = mean(value), count = n(), .groups = 'drop') %>%
		filter(., count == 3) %>%
		select(., -count)

	hist_agg =
		bind_rows(hist_agg_0, monthly_agg, quarterly_agg) %>%
		filter(., freq %in% c('m', 'q'))

	hist_agg <<- hist_agg
})


## 5. Add Stationary Transformations ----------------------------------------------------------
local({

	hist_full =
		hist_agg %>%
		group_split(., sourcename, varname, transform, freq) %>%
		lapply(., function(df_by_var_freq) {

			# Get all available vintage dates for this variable
			vdates = unique(df_by_var_freq$vdate)

			# Get latest obs date for each vintage date
			lapply(vdates, function(this_vdate) {
				prep =
					filter(df_by_var_freq, vdate <= this_vdate) %>%
					group_by(., date) %>%
					filter(., vdate == max(vdate)) %>%
					ungroup(.) %>%
					arrange(., date)
				})


			message(x$varname)

			lapply(c('st', 'd1', 'd2') %>% set_names(., .), function(form) {

				if (x[[form]] == 'none') return(NULL)
				# Now iterate through sub-frequencies
				lapply(x$h$base, function(df)
					df %>%
						dplyr::arrange(., date) %>%
						dplyr::mutate(
							.,
							value = {
								if (x[[form]] == 'base') value
								else if (x[[form]] == 'dlog') dlog(value)
								else if (x[[form]] == 'diff1') diff1(value)
								else if (x[[form]] == 'pchg') pchg(value)
								else if (x[[form]] == 'apchg') apchg(value, {if (x$freq == 'q') 4 else 12})
								else stop ('Error')
							}
						) %>%
						na.omit(.)
				)
			}) %>%
				purrr::compact(.)
		})


})




# External Forecasts ----------------------------------------------------------

## 1. Atlanta Fed ----------------------------------------------------------
local({

	message('1. Atlanta Fed')

	atl_params =
		tribble(
		  ~ varname, ~ fred_id,
		  'gdp', 'GDPNOW',
		  'pce', 'PCENOW',
		  'pdi', 'GDPINOW',
		  'govt', 'GOVNOW',
		  'ex', 'EXPORTSNOW',
		  'im', 'IMPORTSNOW'
		)

	df =
		atl_params %>%
		purrr::transpose(.) %>%
		lapply(., function(x)
			get_fred_data(x$fred_id, CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
			filter(., date >= .$vintage_date - months(3)) %>%
			transmute(
				.,
				fcname = 'atl',
				varname = x$varname,
				transform = 'apchg',
				freq = 'q',
				date,
				vdate = vintage_date,
				value
				)
			) %>%
		bind_rows(.)

	ext$atl <<- df
})


## 2. St. Louis Fed --------------------------------------------------------
local({

	message('2. St. Louis Fed')

	df =
		get_fred_data('STLENI', CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
		dplyr::filter(., date >= .$vintage_date - months(3)) %>%
		dplyr::transmute(
			.,
			fcname = 'stl',
			varname = 'gdp',
			transform = 'apchg',
			freq = 'q',
			date,
			vdate = vintage_date,
			value
			)

	ext$stl <<- df
})


## 3. New York Fed ---------------------------------------------------------
local({

	message('3. New York Fed')

	file = file.path(tempdir(), 'nyf.xlsx')

  httr::GET(
      paste0(
      	'https://www.newyorkfed.org/medialibrary/media/research/policy/nowcast/',
      	'new-york-fed-staff-nowcast_data_2002-present.xlsx'
      	),
      httr::write_disk(file, overwrite = TRUE)
      )

  df =
      readxl::read_excel(file, sheet = 'Forecasts By Quarter', skip = 13) %>%
      dplyr::rename(., vdate = 1) %>%
      dplyr::mutate(., vdate = as_date(vdate)) %>%
      tidyr::pivot_longer(., -vdate, names_to = 'date', values_to = 'value') %>%
      na.omit(.) %>%
      dplyr::mutate(., date = from_pretty_date(date, 'q')) %>%
      dplyr::transmute(
      	.,
      	fcname = 'nyf',
      	varname = 'gdp',
      	transform = 'apchg',
      	freq = 'q',
      	date,
      	vdate,
      	value
      	)

  ext$nyf <<- df
})


## 4. Philadelphia Fed -----------------------------------------------------
local({

	message('4. Philadelphia Fed')

  # Scrape vintage dates
  vintage_dates =
  	httr::GET(paste0('https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
  		'survey-of-professional-forecasters/spf-release-dates.txt?'
  		)) %>%
    httr::content(., as = 'text', encoding = 'UTF-8') %>%
    str_sub(
        .,
        str_locate(., coll('1990 Q2'))[1], str_locate(., coll('*The 1990Q2'))[1] - 1
        ) %>%
  	read_csv(., col_names = 'X1', col_types = 'c') %>%
  	.$X1 %>%
  	purrr::map_dfr(., function(x)
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
  	tidyr::fill(., X1, .direction = 'down') %>%
  	transmute(
          .,
          release_date = from_pretty_date(paste0(X1, X2), 'q'),
          vdate = lubridate::mdy(str_replace_all(str_extract(X3, "[^\\s]+"), '[*]', ''))
      ) %>%
    # Don't include first date - weirdly has same vintage date as second date
    filter(., release_date >= as_date('2000-01-01'))


    spf_params =
      tribble(
        ~ varname, ~ spfname, ~ method, ~ transform,
        'gdp', 'RGDP', 'growth', 'apchg',
        'pce', 'RCONSUM', 'growth', 'apchg',
        'pdin', 'RNRESIN', 'growth', 'apchg',
        'pdir', 'RRESINV', 'growth', 'apchg',
        'govtf', 'RFEDGOV', 'growth', 'apchg',
        'govts', 'RSLGOV', 'growth', 'apchg',
        'ue', 'UNEMP', 'level', 'base',
        't03m', 'TBILL', 'level', 'base',
        't10y', 'TBOND', 'level', 'base',
        'houst', 'HOUSING', 'level', 'base',
        'inf', 'CORECPI', 'level', 'base'
        )


	df = lapply(c('level', 'growth'), function(m) {
		file = file.path(tempdir(), paste0('spf-', m, '.xlsx'))
    httr::GET(
        paste0(
            'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
            'survey-of-professional-forecasters/historical-data/median', m, '.xlsx?la=en'
            ),
            httr::write_disk(file, overwrite = TRUE)
        )
    spf_params %>%
    	filter(., method == m) %>%
    	purrr::transpose(.) %>%
      lapply(., function(x)
        readxl::read_excel(file, na = '#N/A', sheet = x$spfname) %>%
          select(
	          .,
	          c('YEAR', 'QUARTER', {
	            if (m == 'level') paste0(x$spfname, 2:6) else paste0('d', str_to_lower(x$spfname), 2:6)
	            })
	          ) %>%
          mutate(., release_date = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
          select(., -YEAR, -QUARTER) %>%
          tidyr::pivot_longer(., -release_date, names_to = 'fcPeriods') %>%
          mutate(., fcPeriods = as.numeric(str_sub(fcPeriods, -1)) - 2) %>%
          mutate(., date = add_with_rollback(release_date, months(fcPeriods * 3))) %>%
          na.omit(.) %>%
          inner_join(., vintage_dates, by = 'release_date') %>%
          transmute(
          	.,
          	fcname = 'spf',
          	varname = x$varname,
          	freq = 'q',
          	transform = x$transform,
          	vdate,
          	date,
          	value
          	)
        ) %>%
        bind_rows(.) %>%
        return(.)
    }) %>%
    bind_rows(.)

	ext$spf <<- df
})



## 5. WSJ Economic Survey -----------------------------------------------------
# WSJ Survey Updated to Quarterly - see https://www.wsj.com/amp/articles/economic-forecasting-survey-archive-11617814998
local({

	message('5. WSJ Survey')

  wsj_params =
  	tribble(
  		~ fcname, ~ full_fcname,
  		'wsj', 'WSJ Consensus',
  		'fnm', 'Fannie Mae',
  		'wfc', 'Wells Fargo & Co.',
  		'gsu', 'Georgia State University',
  		'sp', 'S&P Global Ratings',
  		'ucla', 'UCLA Anderson Forecast',
  		'gs', 'Goldman, Sachs & Co.',
  		'ms', 'Morgan Stanley'
  	)

  file_paths =
  	tribble(
  		~ vdate, ~ file,
  		'2021-04-11', 'wsjecon0421.xls',
			'2021-07-11', 'wsjecon0721.xls',
			'2021-10-17', 'wsjecon1021.xls'
			# October 17th next
  		) %>%
  	purrr::transpose(.)

  df = lapply(file_paths, function(x) {
	  message(x$date)
	  dest = file.path(tempdir(), 'wsj.xls')

    # A user-agent is required or garbage is returned
    httr::GET(
        paste0('https://online.wsj.com/public/resources/documents/', x$file),
        httr::write_disk(dest, overwrite = TRUE),
        httr::add_headers(
        	'Host' = 'online.wsj.com',
        	'User-Agent' = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
        	)
        )

    # Read first two lines to parse column names
    xlDf = suppressMessages(readxl::read_excel(dest, col_names = FALSE, .name_repair = 'unique'))

    # Create new column names
    xlRepair =
      xlDf %>%
      {tibble(colname = unlist(.[1, ]), date = unlist(.[2, ]))} %>%
      tidyr::fill(., colname) %>%
      mutate(
	      .,
	      varname = str_to_lower(colname),
	      # https://stackoverflow.com/questions/4389644/regex-to-match-string-containing-two-names-in-any-order
	      # Repair varnames
	      varname = ifelse(str_detect(date, 'Organization'), 'fcfullname', varname),
	      varname = ifelse(str_detect(varname, '(?=.*gdp)(?=.*quarterly)'), 'gdp', varname),
	      varname = ifelse(str_detect(varname, '(?=.*fed)(?=.*funds)'), 'ffr', varname),
	      varname = ifelse(str_detect(varname, 'cpi'), 'inf', varname),
	      varname = ifelse(str_detect(varname, 'unemployment'), 'ue', varname),
	      # Keep varname
	      keep = varname %in% c('fcfullname', 'gdp', 'ffr', 'inf', 'ue')
	      ) %>%
      mutate(
        .,
        # Repair dates
        date = str_replace_all(
        	date,
          c(
            'Fourth Quarter ' = '10 ', 'Third Quarter ' = '7 ',
            'Second Quarter ' = '4 ', 'First Quarter ' = '1 ',
            setNames(paste0(1:12, ' '), paste0(month.abb, ' ')),
            setNames(paste0(1:12, ' '), paste0(month.name, ' '))
            )
          ),
        date =
	        ifelse(
            !date %in% c('Name:', 'Organization:') & keep == TRUE,
            paste0(
	            str_sub(date, -4),
	            '-',
	            str_pad(str_squish(str_sub(date, 1, nchar(date) - 4)), 2, pad = '0'),
	            '-01'
	            ),
            NA
            ),
        date = as_date(date)
        )


    df =
	    suppressMessages(readxl::read_excel(
        dest,
        col_names = paste0(xlRepair$varname, '_', xlRepair$date),
        na = c('', ' ', '-', 'NA'),
        skip = 2
        )) %>%
      # Only keep columns selected as keep = TRUE in last step
      select(
        .,
        xlRepair %>% mutate(., index = 1:nrow(.)) %>% filter(., keep == TRUE) %>% .$index
        ) %>%
      rename(., 'full_fcname' = 1) %>%
      # Bind WSJ row - select last vintage
      mutate(., full_fcname = ifelse(full_fcname %in% month.name, 'WSJ Consensus', full_fcname)) %>%
      filter(., full_fcname %in% wsj_params$full_fcname) %>%
      {
        bind_rows(
          filter(., !full_fcname %in% 'WSJ Consensus'),
          filter(., full_fcname %in% 'WSJ Consensus') %>% head(., 1)
        )
      } %>%
      mutate(., across(-full_fcname, as.numeric)) %>%
      pivot_longer(., -full_fcname, names_sep = '_', names_to = c('varname', 'date')) %>%
      mutate(., date = as_date(date)) %>%
      # Now split and fill in frequencies and quarterly data
      group_by(., full_fcname, varname) %>%
      group_split(.) %>%
  		purrr::keep(., function(z) nrow(filter(z, !is.na(value))) > 0 ) %>% # Cull completely empty data frames
      purrr::map_dfr(., function(z)
        tibble(
          date = seq(from = min(na.omit(z)$date), to = max(na.omit(z)$date), by = '3 months')
          ) %>%
          left_join(., z, by = 'date') %>%
          mutate(., value = zoo::na.approx(value)) %>%
          mutate(
            .,
            full_fcname = unique(z$full_fcname),
            varname = unique(z$varname),
            freq = 'q',
            transform = case_when(
            	varname == 'gdp' ~ 'apchg',
            	varname == 'ffr' ~ 'base',
            	varname == 'inf' ~ 'base',
            	varname == 'ue' ~ 'base'
            	),
            vdate = as_date(x$vdate)
            )
        ) %>%
      right_join(., wsj_params, by = 'full_fcname') %>%
      select(., -full_fcname)
    }) %>%
    bind_rows(.) %>%
  	na.omit(.)


  	# Verify
	  df %>%
	  	group_split(., varname) %>%
	  	setNames(., map(., ~.$varname[[1]])) %>%
	  	lapply(., function(x) pivot_wider(x, id_cols = c('fcname', 'vdate'), names_from = 'date'))


    ext$wsj <<- df
})



## 6. CBO Forecasts --------------------------------------------------------
local({

	message('6. CBO')

  url_params =
    httr::GET('https://www.cbo.gov/data/budget-economic-data') %>%
    httr::content(., type = 'parsed') %>%
    xml2::read_html(.) %>%
    rvest::html_nodes('div .view-content') %>%
    .[[9]] %>%
    rvest::html_nodes(., 'a') %>%
    purrr::map_dfr(., function(x) tibble(date = rvest::html_text(x), url = rvest::html_attr(x, 'href'))) %>%
    transmute(
      .,
      date =
        paste0(
          str_sub(date, -4), '-',
          str_pad(match(str_sub(date, 1, 3), month.abb), 2, pad = '0'),
          '-01'
          ),
      url
    	) %>%
    mutate(., date = as_date(date)) %>%
    filter(., date >= as_date('2020-01-01'))


  temp_path = file.path(tempdir(), 'cbo.xlsx')

  cbo_params =
    tribble(
      ~ varname, ~ cbo_category, ~ cbo_name, ~ cbo_units, ~ transform,

      'gdp', 'Output', 'Real GDP', 'Percentage change, annual rate', 'apchg',

      'inf', 'Prices', 'Consumer Price Index, All Urban Consumers (CPI-U)', 'Percentage change, annual rate',
      'base',

      'oil', 'Prices', 'Price of Crude Oil, West Texas Intermediate (WTI)', 'Dollars per barrel', 'base',

      'ue', 'Labor', 'Unemployment Rate, Civilian, 16 Years or Older', 'Percent', 'base',

      'ffr', 'Interest Rates', 'Federal Funds Rate', 'Percent', 'base',

      't10y', 'Interest Rates', '10-Year Treasury Note', 'Percent', 'base',

      't03m', 'Interest Rates', '3-Month Treasury Bill', 'Percent', 'base',

      'pce', 'Components of GDP (Real)', 'Personal Consumption Expenditures',
      'Percentage change, annual rate', 'apchg',

      'pdi', 'Components of GDP (Real)', 'Gross Private Domestic Investment',
      'Percentage change, annual rate', 'apchg',

      'pdin', 'Components of GDP (Real)', 'Nonresidential fixed investment',
      'Percentage change, annual rate', 'apchg',

      'pdir', 'Components of GDP (Real)', 'Residential fixed investment',
      'Percentage change, annual rate', 'apchg',

      'govt', 'Components of GDP (Real)', 'Government Consumption Expenditures and Gross Investment',
    	'Percentage change, annual rate', 'apchg',

      'govtf', 'Components of GDP (Real)', 'Federal', 'Percentage change, annual rate', 'apchg',

      'govts', 'Components of GDP (Real)', 'State and local', 'Percentage change, annual rate', 'apchg',

      'ex', 'Components of GDP (Real)', 'Exports', 'Percentage change, annual rate', 'apchg',

      'im', 'Components of GDP (Real)', 'Imports', 'Percentage change, annual rate', 'apchg'
      )


  df =
    url_params %>%
    purrr::transpose(.) %>%
    lapply(., function(x) {

      download.file(x$url, temp_path, mode = 'wb', quiet = TRUE)

      # Starts earlier form Jan 2019
      xl =
        suppressMessages(readxl::read_excel(
          temp_path,
          sheet = '1. Quarterly',
          skip = 6
          )) %>%
        rename(., cbo_category = 1, cbo_name = 2, cbo_name_2 = 3, cbo_units = 4) %>%
        mutate(., cbo_name = ifelse(is.na(cbo_name), cbo_name_2, cbo_name)) %>%
        select(., -cbo_name_2) %>%
        tidyr::fill(., cbo_category, .direction = 'down') %>%
        tidyr::fill(., cbo_name, .direction = 'down') %>%
        na.omit(.)

      xl %>%
        inner_join(., cbo_params, by = c('cbo_category', 'cbo_name', 'cbo_units')) %>%
        select(., -cbo_category, -cbo_name, -cbo_units) %>%
        pivot_longer(., cols = -c('varname', 'transform'), names_to = 'date') %>%
        mutate(., date = from_pretty_date(date, 'q')) %>%
        filter(., date >= as_date(x$date)) %>%
        mutate(., vdate = as_date(x$date))
      }) %>%
    bind_rows(.) %>%
    transmute(., fcname = 'cbo', varname, transform, freq = 'q', date, vdate, value)

  # Count number of forecasts per group
  # df %>% dplyr::group_by(vintageDate, varname) %>% dplyr::summarize(., n = n()) %>% View(.)

  ext$cbo <<- df
})


## 7. Cleveland Fed (Expected Inf) -----------------------------------------
local({

	message('7. Cleveland Fed')

	data_sources = tibble(
		years_forward = 1:20,
		fred_id = paste0('EXPINF', 1:20, 'YR')
		)

	data_import = purrr::imap_dfr(purrr::transpose(data_sources), function(x, i) {
		message(str_glue('Pull {i}: {x$fred_id}'))
		get_fred_data(
			x$fred_id,
			CONST$FRED_API_KEY,
			.freq = 'm',
			.return_vintages = TRUE,
			.verbose = F
			) %>%
			transmute(
				.,
				date,
				vdate = vintage_date,
				ttm = x$years_forward * 12,
				value
				)
	})


  file = file.path(tempdir(), paste0('inf.xls'))

  download.file(
  	paste0(
  		'https://www.clevelandfed.org/en/our-research/indicators-and-data/~/media/content/our%20research/',
  		'indicators%20and%20data/inflation%20expectations/ie%20latest/ie%20xls.xls'
  		),
  	file,
  	mode = 'wb'
  	)

  einf_final =
  	readxl::read_excel(file, sheet = 'Expected Inflation') %>%
  	rename(., vdate = 'Model Output Date') %>%
  	pivot_longer(., -vdate, names_to = 'ttm', values_to = 'yield') %>%
  	mutate(
  		.,
  		vdate = as_date(vdate), ttm = as.numeric(str_replace(str_sub(ttm, 1, 2), ' ', '')) * 12
  	) %>%
  	filter(., vdate >= as_date('2015-01-01')) %>%
  	group_split(., vdate) %>%
  	map_dfr(., function(x)
  		right_join(x, tibble(ttm = 1:360), by = 'ttm') %>%
  			arrange(., ttm) %>%
  			mutate(
  				.,
  				yield = zoo::na.spline(yield),
  				vdate = unique(na.omit(vdate)),
  				cur_date = floor_date(vdate, 'months'),
  				cum_return = (1 + yield)^(ttm/12),
  				yttm_ahead_cum_return = dplyr::lead(cum_return, 1)/cum_return,
  				yttm_ahead_annualized_yield = (yttm_ahead_cum_return ^ 12 - 1) * 100,
  				date = add_with_rollback(cur_date, months(ttm - 1))
  				)
  		) %>%
  	transmute(
  		.,
			fcname = 'cle',
			varname = 'inf',
      transform = 'base',
			freq = 'm',
			date,
  		vdate,
  		value = yttm_ahead_annualized_yield,
  		) %>%
  	na.omit(.)

  einf_final %>%
		filter(., month(vdate) %in% c(1, 6)) %>%
		ggplot(.) + geom_line(aes(x = date, y = value, color = as.factor(vdate)))

  ext$cle <<- df
})


## 8. CME ---------------------------------------------------------------------
local({

	message('8. CME Forecasts')

	# First get from Quandl
	message('Starting Quandl data scrape...')
	df =
		lapply(1:24, function(j) {
			# message(j)
			read_csv(
				paste0(
					'https://www.quandl.com/api/v3/datasets/CHRIS/CME_FF', j,
					'.csv?api_key=', CONST$QUANDL_API_KEY
					),
				col_types = 'Ddddddddd'
				) %>%
				transmute(., vdate = Date, settle = Settle, j = j) %>%
				filter(., vdate >= as.Date('2010-01-01'))
			}) %>%
		bind_rows(.) %>%
		transmute(
			.,
			vdate,
			# Consider the forecasted period the vdate + j
			date =
				from_pretty_date(paste0(year(vdate), 'M', month(vdate)), 'm') %>%
				add_with_rollback(., months(j - 1), roll_to_first = TRUE),
			value = 100 - settle,
			varname = 'ffr',
			fcname = 'cme'
			)

	message('Completed Quandl data scrape')

	message('Starting CME data scrape...')

	cookie =
    httr::GET(
	    'https://www.cmegroup.com/',
      add_headers(c(
	      'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
	      'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
	      'Accept-Encoding' = 'gzip, deflate, br',
	      'Accept-Language' ='en-US,en;q=0.5',
	      'Cache-Control'='no-cache',
	      'Connection'='keep-alive',
	      'DNT' = '1'
	      ))
	    ) %>%
		httr::cookies(.) %>%
		as_tibble(.) %>%
		filter(., name == 'ak_bmsc') %>%
		.$value


	# Get CME Vintage Date
	last_trade_date =
		httr::GET(
			paste0('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/305/G?quoteCodes=null&_='),
			add_headers(c(
				'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
				'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				'Accept-Encoding' = 'gzip, deflate, br',
				'Accept-Language' ='en-US,en;q=0.5',
				'Cache-Control'='no-cache',
				'Connection'='keep-alive',
				'Cookie'= cookie,
				'DNT' = '1',
				'Host' = 'www.cmegroup.com'
			))
		) %>% content(., 'parsed') %>% .$tradeDate %>% lubridate::parse_date_time(., 'd-b-Y') %>% as_date(.)


	df2 =
		tribble(
			~ varname, ~ cmeId,
			'ffr', '305',
			'sofr', '8462',
			'sofr', '8463'
		) %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(var) {
			# message(var)
			content =
				httr::GET(
					paste0('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/', var$cmeId, '/G?quoteCodes=null&_='),
					# 3/30/21 - CME website now requires user-agent header
					add_headers(c(
						'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
						'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
						'Accept-Encoding' = 'gzip, deflate, br',
						'Accept-Language' ='en-US,en;q=0.5',
						'Cache-Control'='no-cache',
						'Connection'='keep-alive',
						'Cookie'= cookie,
						'DNT' = '1',
						'Host' = 'www.cmegroup.com'
						))
					) %>%
					httr::content(., as = 'parsed')

			content %>%
				.$quotes %>%
				purrr::map_dfr(., function(x) {
					if (x$priorSettle %in% c('0.00', '-')) return() # Whack bug in CME website
					tibble(
						date = lubridate::ymd(x$expirationDate),
						value = 100 - as.numeric(x$priorSettle),
						varname = var$varname
						)
					}) %>%
				return(.)
			}) %>%
		# Now average out so that there's only one value for each (varname, date) combo
		group_by(varname, date) %>%
		summarize(., value = mean(value), .groups = 'drop') %>%
		arrange(., date) %>%
		# Get rid of forecasts for old observations
		filter(., date >= lubridate::floor_date(Sys.Date(), 'month')) %>%
		# Assume vintagedate is the same date as the last Quandl obs
		mutate(., vdate = last_trade_date, fcname = 'cme') %>%
		filter(., value != 100)


	message('Completed CME data scrape...')

	# Now combine, replacing df2 with df1 if necessary
	combined_df =
		full_join(df, df2, by = c('fcname', 'vdate', 'date', 'varname')) %>%
		# Use quandl data if available, otherwise use other data
		mutate(., value = ifelse(!is.na(value.x), value.x, value.y)) %>%
		select(., -value.x, -value.y)

	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
	# 	df %>%
	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
	# 		View(.)


	## Add monthly interpolation
	message('Adding monthly interpolation ...')

	final_df =
		combined_df %>%
		group_by(vdate, varname) %>%
		group_split(.) %>%
		lapply(., function(x) {
			x %>%
				# Join on missing obs dates
				right_join(
					.,
					tibble(date = seq(from = .$date[[1]], to = tail(.$date, 1), by = '1 month')) %>%
						mutate(n = 1:nrow(.)),
					by = 'date'
					) %>%
				arrange(date) %>%
				transmute(
					.,
					fcname = head(fcname, 1),
					varname = head(varname, 1),
					transform = 'base',
					freq = 'm',
					date = date,
					vdate = head(vdate, 1),
					value = zoo::na.spline(value)
					)
			}) %>%
		dplyr::bind_rows(.)

	ext$cme <<- final_df
})

## 9. DNS - TDNS1, TDNS2, TDNS3, Treasury Yields, Spreads ---------------------
local({

	message('9. DNS Forecasts')

	fred_data =
		variable_params %>%
		filter(., str_detect(fullname, 'Treasury Yield') | varname == 'ffr') %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(x) {
			get_fred_data(x$sckey, CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
				transmute(., varname = x$varname, date, vdate = vintage_date, value) %>%
				filter(., date >= as_date('2010-01-01'))
		})

	# Monthly aggregation & append EOM with current val
	fred_data_cat =
		fred_data %>%
		group_split(., varname) %>%
		# Add monthly values for current month
		map_dfr(., function(x)
			x %>%
				mutate(., date = as.Date(paste0(year(date), '-', month(date), '-01'))) %>%
				group_by(., varname, date) %>%
				summarize(., value = mean(value), .groups = 'drop') %>%
				mutate(., freq = 'm')
			)

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yield_curve_names_map =
		variable_params %>%
		purrr::transpose(.) %>%
		map_chr(., ~.$varname) %>%
		unique(.) %>%
		purrr::keep(., ~ str_sub(., 1, 1) == 't' & str_length(.) == 4) %>%
		tibble(varname = .) %>%
		mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))


	# Create training dataset from SPREAD from ffr - fitted on last 3 months
	train_df =
		filter(fred_data_cat, varname %in% yield_curve_names_map$varname) %>%
		select(., -freq) %>%
		filter(., date >= add_with_rollback(Sys.Date(), months(-3))) %>%
		right_join(., yield_curve_names_map, by = 'varname') %>%
		left_join(., transmute(filter(fred_data_cat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		mutate(., value = value - ffr) %>%
		select(., -ffr)

	#' Calculate DNS fit
	#'
	#' @param df: (tibble) A tibble continuing columns obsDate, value, and ttm
	#' @param return_all: (boolean) FALSE by default.
	#' If FALSE, will return only the MAPE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @export
	get_dns_fit = function(df, lambda, return_all = FALSE) {
		df %>%
			mutate(
				.,
				f1 = 1,
				f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm),
				f3 = f2 - exp(-1 * lambda * ttm)
			) %>%
			group_by(date) %>%
			group_split(.) %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				bind_cols(x, fitted = fitted(reg)) %>%
					mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
					mutate(., resid = value - fitted)
			}) %>%
			bind_rows(.) %>%
			{
				if (return_all == FALSE) summarise(., mse = mean(abs(resid))) %>% .$mse
				else .
			} %>%
			return(.)
	}

	# Find MSE-minimizing lambda value
	optim_lambda =
		optimize(
			get_dns_fit,
			df = train_df,
			return_all = FALSE,
			interval = c(-1, 1),
			maximum = FALSE
		)$minimum

	# Store DNS coefficients
	dns_coefs =
		get_dns_fit(df = train_df, optim_lambda, return_all = TRUE) %>%
		filter(., date == max(date)) %>%
		select(., b1, b2, b3) %>%
		head(., 1) %>%
		as.list(.)

	dns_fit =
		get_dns_fit(df = train_df, optim_lambda, return_all = TRUE) %>%
		filter(., date == max(date)) %>%
		arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted))


	print(dns_fit)


	# DIEBOLD LI FUNCTION SHOULD BE ffr + f1 + f2 () + f3()
	# Calculated TDNS1: TYield_10y
	# Calculated TDNS2: -1 * (t10y - t03m)
	# Calculated TDNS3: .3 * (2*t02y - t03m - t10y)
	# Keep these treasury yield forecasts as the external forecasts ->
	# note that later these will be "regenerated" in the baseline calculation,
	# may be off a bit due to calculation from TDNS, compare to

  # Monthly forecast up to 10 years
  # Get cumulative return starting from cur_date
  fitted_curve =
      tibble(ttm = seq(1: 480)) %>%
      dplyr::mutate(., cur_date = floor_date(today(), 'months')) %>%
      dplyr::mutate(
        .,
        annualized_yield =
        	dns_coefs$b1 +
        	dns_coefs$b2 * (1-exp(-1 * optim_lambda * ttm))/(optim_lambda * ttm) +
        	dns_coefs$b3 * ((1-exp(-1 * optim_lambda * ttm))/(optim_lambda * ttm) - exp(-1 * optim_lambda * ttm)),
        # Get dns_coefs yield
        cum_return = (1 + annualized_yield/100)^(ttm/12)
        )


  # Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
  # and for each, iterate over the original "ttms" 1, 2, 3,
  # ..., 120 and for each forecast the cumulative return for the yttm period ahead.
  df0 =
  	yield_curve_names_map$ttm %>%
    lapply(., function(yttm)
	  	fitted_curve %>%
        mutate(
          .,
          yttmAheadCumReturn = dplyr::lead(cum_return, yttm)/cum_return,
          yttmAheadAnnualizedYield = (yttmAheadCumReturn^(12/yttm) - 1) * 100
          ) %>%
        filter(., ttm <= 120) %>%
        mutate(., yttm = yttm) %>%
        inner_join(., yield_curve_names_map, c('yttm' = 'ttm'))
        ) %>%
      bind_rows(.) %>%
      mutate(
        .,
        date = add_with_rollback(cur_date, months(ttm - 1))
        ) %>%
      transmute(
        .,
        fcname = 'dns',
        varname,
        date,
        transform = 'base',
        freq = 'm',
        vdate = today(),
        value = yttmAheadAnnualizedYield
        )

    # Add ffr to forecasts
  df1 =
    df0 %>%
    select(., varname, date, value) %>%
    inner_join(
      .,
    	ext$cme %>%
        filter(., vdate == max(vdate)) %>%
        filter(., varname == 'ffr') %>%
        transmute(., ffr = value, date),
      by = 'date'
    	) %>%
    mutate(., value = value + ffr) %>%
    transmute(
      .,
      fcname = 'dns',
      varname,
      date,
      transform = 'base',
      freq = 'm',
      vdate = today(),
      value
      )

  # Calculate TDNS yield forecasts
  # Forecast vintage date should be bound to historical data vintage
  # date since reliant purely on historical data
  df2 =
    df0 %>%
    select(., varname, date, value) %>%
    tidyr::pivot_wider(., names_from = 'varname') %>%
    transmute(
        .,
        date,
        tdns1 = t10y,
        tdns2 = -1 * (t10y - t03m),
        tdns3 = .3 * (2 * t02y - t03m - t10y)
        ) %>%
    tidyr::pivot_longer(., -date, names_to = 'varname') %>%
    transmute(., fcname = 'dns', varname, date, transform = 'base', freq = 'm', vdate = today(), value)

  ext$dns <<- bind_rows(df1, df2)
})

## 10. Combine and Flatten -------------------------------------------------
local({

	message('10. Combine and Flatten')

	flat =
		ext %>%
		bind_rows(.) %>%
		transmute(
			.,
			fcname,
			vdate,
			freq,
			transform,
			varname,
			date,
			value
			)

	if (nrow(na.omit(flat)) != nrow(flat)) stop('Missing obs')

	ext_final <<- flat
})

# Send to SQL  -------------------------------------------------

## 1. Reset   -------------------------------------------------
local({

	if (RESET_SQL) {

		dbExecute(db, 'DROP TABLE IF EXISTS external_forecast_values CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS external_forecast_names CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS historical_values CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS variable_params CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS variable_releases CASCADE')
	}

})


## 1. Releases  -------------------------------------------------
local({

	if (!'variable_releases' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(
			db,
			'CREATE TABLE variable_releases (
				relkey VARCHAR(255) CONSTRAINT relkey_pk PRIMARY KEY,
				relname VARCHAR(255) CONSTRAINT relname_uk UNIQUE NOT NULL,
				relsc VARCHAR(255) NOT NULL,
				relsckey VARCHAR(255) ,
				relurl VARCHAR(255),
				relnotes TEXT,
				n_varnames INTEGER,
				varnames JSON,
				n_dfm_varnames INTEGER,
				dfm_varnames JSON,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				)'
			)
	}

	releases_final %>%
		transmute(
			.,
			relkey, relname, relsc, relsckey , relurl, relnotes,
			n_varnames = ifelse(is.na(n_varnames), 0, n_varnames), varnames,
			n_dfm_varnames = ifelse(is.na(n_dfm_varnames), 0, n_dfm_varnames), dfm_varnames
			) %>%
		create_insert_query(
			.,
			'variable_releases',
			str_squish(
				'ON CONFLICT ON CONSTRAINT relkey_pk DO UPDATE
		    SET
		    relname=EXCLUDED.relname,
		    relsc=EXCLUDED.relsc,
		    relsckey=EXCLUDED.relsckey,
		    relurl=EXCLUDED.relurl,
		    relnotes=EXCLUDED.relnotes,
		    n_varnames=EXCLUDED.n_varnames,
		    varnames=EXCLUDED.varnames,
		    n_dfm_varnames=EXCLUDED.n_dfm_varnames,
		    dfm_varnames=EXCLUDED.dfm_varnames'
				)
			) %>%
		dbSendQuery(db, .)
})

## 2. Variables  -------------------------------------------------
local({

	if (!'variable_params' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(
			db,
			'CREATE TABLE variable_params (
				varname VARCHAR(255) CONSTRAINT varname_pk PRIMARY KEY,
				fullname VARCHAR(255) CONSTRAINT fullname_uk UNIQUE NOT NULL,
				category VARCHAR(255) NOT NULL,
				dispgroup VARCHAR(255),
				disprank INT,
				disptabs INT,
				disporder INT,
				source VARCHAR(255) NOT NULL,
				sckey VARCHAR(255),
				relkey VARCHAR(255) NOT NULL,
				units VARCHAR(255) NOT NULL,
				freq CHAR(1) NOT NULL,
				sa CHAR(1) NOT NULL,
				st VARCHAR(50) NOT NULL,
				st2 VARCHAR(50) NOT NULL,
				d1 VARCHAR(50) NOT NULL,
				d2 VARCHAR(50) NOT NULL,
				nc_dfm_input BOOLEAN,
				nc_method VARCHAR(50) NOT NULL,
				initial_forecast VARCHAR(50),
				core_structural VARCHAR(50) NOT NULL,
				core_endog_type VARCHAR(50),
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT relkey_fk FOREIGN KEY (relkey) REFERENCES variable_releases (relkey)
					ON DELETE CASCADE ON UPDATE CASCADE
				)'
			)
	}

	variable_params %>%
		transmute(
			.,
			varname, fullname, category, dispgroup, disprank, disptabs, disporder,
			source, sckey, relkey, units, freq,
			sa, st, st2, d1, d2, nc_dfm_input, nc_method, initial_forecast,
			core_structural, core_endog_type
			) %>%
		create_insert_query(
			.,
			'variable_params',
			str_squish(
				'ON CONFLICT ON CONSTRAINT varname_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    category=EXCLUDED.category,
		    dispgroup=EXCLUDED.dispgroup,
		    disprank=EXCLUDED.disprank,
		    disptabs=EXCLUDED.disptabs,
		    disporder=EXCLUDED.disporder,
		    source=EXCLUDED.source,
		    sckey=EXCLUDED.sckey,
		    relkey=EXCLUDED.relkey,
		    units=EXCLUDED.units,
		    freq=EXCLUDED.freq,
		    sa=EXCLUDED.sa,
		    st=EXCLUDED.st,
		    st2=EXCLUDED.st2,
		    d1=EXCLUDED.d1,
		    d2=EXCLUDED.d2,
		    nc_dfm_input=EXCLUDED.nc_dfm_input,
		    nc_method=EXCLUDED.nc_method,
		    initial_forecast=EXCLUDED.initial_forecast,
		    core_structural=EXCLUDED.core_structural,
		    core_endog_type=EXCLUDED.core_endog_type'
				)
			) %>%
		dbSendQuery(db, .)

})


## 3. External Forecast Names -------------------------------------------------
local({

	if (!'external_forecast_names' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(
			db,
			'CREATE TABLE external_forecast_names (
				tskey VARCHAR(10) CONSTRAINT external_forecast_names_pk PRIMARY KEY,
				forecast_type VARCHAR(255),
				shortname VARCHAR(100) NOT NULL,
				fullname VARCHAR(255) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				)'
			)
		dbExecute(db, 'CREATE INDEX external_forecast_names_ix_fctype ON external_forecast_names (forecast_type)')
	}

	external_forecast_names =
		tribble(
			~ tskey, ~ forecast_type, ~ shortname, ~ fullname,
			# 1
			'atl', 'quant', 'Atlanta Fed', 'Atlanta Fed GDPNow Model',
			# 2
			'stl', 'qual', 'St. Louis Fed', 'St. Louis Fed Economic News Model',
			# 3
			'nyf', 'qual', 'NY Fed', 'New York Fed Staff Nowcast',
			# 4
			'spf', 'qual', 'Survey of Professional Forecasters', 'Survey of Professional Forecasters',
			# 5
			'wsj', 'qual', 'WSJ Consensus', 'Wall Street Journal Consensus Forecast',
			'fnm', 'qual', 'Fannie Mae', 'Fannie Mae Forecast',
			'wfc', 'qual', 'Wells Fargo', 'Wells Fargo Forecast',
			'gsu', 'qual', 'GSU', 'Georgia State University Forecast',
			'sp', 'qual', 'S&P', 'S&P Global Ratings',
			'ucla', 'qual', 'UCLA Anderson', 'UCLA Anderson Forecast',
			'gs', 'qual', 'Goldman Sachs', 'Goldman Sachs Forecast',
			'ms', 'qual', 'Morgan Stanley', 'Morgan Stanley',
			# 6
			'cbo', 'quant', 'CBO', 'Congressional Budget Office Projections',
			# 7
			'cle', 'fut', 'Futures-Implied Inflation Rates', 'Futures-Implied Expected Inflation Model',
			# 8
			'cme', 'fut', 'Futures-Implied Interest Rates', 'Futures-Implied Expected Benchmark Rates Model',
			# 9
			'dns', 'fut', 'Futures-Implied Treasury Yield', 'Futures-Implied Expected Treasury Yields Model'
			)

	ext_final %>% group_by(., fcname) %>% summarize(., n = n())

	external_forecast_names %>%
		create_insert_query(
			.,
			'external_forecast_names',
			str_squish(
				'ON CONFLICT ON CONSTRAINT external_forecast_names_pk DO UPDATE
		    SET
		    forecast_type=EXCLUDED.forecast_type,
		    shortname=EXCLUDED.shortname,
		    fullname=EXCLUDED.fullname'
				)
			) %>%
		dbSendQuery(db, .)
})


## 4. Forecast Values ------------------------------------------------
local({

	if (!'external_forecast_values' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {

		dbExecute(
			db,
			'CREATE TABLE external_forecast_values (
				tskey VARCHAR(10) NOT NULL,
				vdate DATE NOT NULL,
				freq CHAR(1) NOT NULL,
				transform VARCHAR(255) NOT NULL,
				varname VARCHAR(255) NOT NULL,
				date DATE NOT NULL,
				value NUMERIC(20, 4) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT external_forecast_values_pk PRIMARY KEY (tskey, vdate, freq, transform, varname, date),
				CONSTRAINT external_forecast_values_fk FOREIGN KEY (tskey) REFERENCES external_forecast_names (tskey)
					ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT ext_tsvalues_varname_fk FOREIGN KEY (varname) REFERENCES variable_params (varname)
					ON DELETE CASCADE ON UPDATE CASCADE
				)'
			)

		dbExecute(db, '
			SELECT create_hypertable(
				relation => \'external_forecast_values\',
				time_column_name => \'vdate\'
				);
			')
	}

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		ext_final %>%
		transmute(., tskey = fcname, vdate, freq, transform, varname, date, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_by(., split) %>%
		group_split(., .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'external_forecast_values',
				str_squish(
					'ON CONFLICT (tskey, vdate, freq, transform, varname, date) DO UPDATE
			    SET value=EXCLUDED.value'
					)
				) %>%
				DBI::dbExecute(db, .)
			) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}


	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL:')
	print(sum(unlist(sql_result)))

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_forecast_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)

})

## 5. Historical Values ------------------------------------------------
local({

	if (!'historical_values' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {

		dbExecute(
			db,
			'CREATE TABLE historical_values (
				varname VARCHAR(255) NOT NULL,
				transform VARCHAR(255) NOT NULL,
				freq CHAR(1) NOT NULL,
				date DATE NOT NULL,
				vdate DATE NOT NULL,
				value NUMERIC(20, 4) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT historical_values_pk PRIMARY KEY (varname, transform, freq, date, vdate),
				CONSTRAINT historical_values_varname_fk FOREIGN KEY (varname) REFERENCES variable_params (varname)
					ON DELETE CASCADE ON UPDATE CASCADE
				)'
			)

		dbExecute(db, '
			SELECT create_hypertable(
				relation => \'historical_values\',
				time_column_name => \'vdate\'
				);
			')
	}

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM historical_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		hist_final %>%
		transmute(., varname, transform, freq, date, vdate, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_by(., split) %>%
		group_split(., .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'historical_values',
				str_squish(
					'ON CONFLICT (varname, transform, freq, date, vdate) DO UPDATE
			    SET value=EXCLUDED.value'
					)
				) %>%
				DBI::dbExecute(db, .)
			) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}


	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL:')
	print(sum(unlist(sql_result)))

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM historical_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)

})
