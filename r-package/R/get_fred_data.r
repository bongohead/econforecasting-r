#' Returns data from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param series_id The FRED identifier of the time series to pull.
#' @param api_key A valid FRED API key.
#' @param .freq One of 'd', 'm', 'q'. If NULL, returns highest available frequency.
#' @param .return_vintages If TRUE, returns all historic values ('vintages').
#' @param .vintage_date If .return_vintages = TRUE, .vintage_date can be set to only return the vintage for a single date.
#' @param .obs_start The default start date of results to return.
#' @param .verbose If TRUE, echoes error messages.
#'
#' @return A data frame of data
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date
#'
#' @export
get_fred_data = function(series_id, api_key, .freq = NULL, .return_vintages = FALSE, .vintage_date = NULL, .obs_start = '2000-01-01', .verbose = FALSE) {

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	url = paste0(
		'https://api.stlouisfed.org/fred/series/observations?',
		'series_id=', series_id,
		'&api_key=', api_key,
		'&file_type=json',
		'&realtime_start=', if (.return_vintages == TRUE & is.null(.vintage_date)) .obs_start else if (.return_vintages == TRUE & !is.null(.vintage_date)) .vintage_date else today,
		'&realtime_end=', if (.return_vintages == TRUE & !is.null(.vintage_date)) .vintage_date else today,
		'&observation_start=', .obs_start,
		'&observation_end=', today,
		if(!is.null(.freq)) paste0('&frequency=', .freq) else '',
		'&aggregation_method=avg'
		)

	if (.verbose == TRUE) message(url)

	request(url) %>%
		req_perform %>%
		resp_body_json %>%
		.$observations %>%
		map(., as_tibble) %>%
		list_rbind %>%
		filter(., value != '.') %>%
		na.omit %>%
		transmute(
			.,
			date = as_date(date),
			vintage_date = as_date(realtime_start),
			varname = series_id,
			value = as.numeric(value)
		) %>%
		{if(.return_vintages == TRUE) . else select(., -vintage_date)} %>%
		return(.)
}



#' Returns observations for all available vintages from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param series_id The FRED identifier of the time series to pull.
#' @param api_key A valid FRED API key.
#' @param .freq One of 'd', 'w', 'm', 'q'.
#' @param .obs_start The default start date of results to return.
#' @param .verbose If TRUE, echoes error messages.
#'
#' @return A data frame of data
#'
#' @description
#' This is an improvement of the original get_fred_data with simplified parameters.
#'  It now always returns vintages and also automatically breaks up vintage dates into chunks to avoid FRED errors.
#'  Note that since FRED returns weekly data in the form of week-ending dates, we subtract 7 days off the date to
#'  convert them into week beginning dates.
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date
#'
#' @export
get_fred_obs_with_vintage = function(series_id, api_key, .freq, .obs_start = '2000-01-01', .verbose = F)  {

	today = as_date(with_tz(now(), tz = 'America/Chicago'))
	max_vdates_per_fetch = 2000 # Limit imposed by FRED

	vintage_dates_url = paste0(
		'https://api.stlouisfed.org/fred/series/vintagedates?',
		'series_id=', series_id,
		'&api_key=', api_key,
		'&file_type=json',
		'&realtime_start=', .obs_start,
		'&realtime_end=', today
		)

	get_vintage_dates = function(vintage_dates_url) {
		response = req_perform(request(vintage_dates_url))
		if (resp_is_error(response)) stop('Error')
		unlist(resp_body_json(response)$vintage_dates)
	}

	vintage_dates = insistently(get_vintage_dates, rate = rate_delay(30, 10), quiet = !.verbose)(vintage_dates_url)

	vintage_date_groups = map(split(vintage_dates, ceiling(seq_along(vintage_dates)/max_vdates_per_fetch)), \(x) list(
		start = head(x, 1),
		end = tail(x, 1)
		))

	obs_urls = map(vintage_date_groups, \(x) paste0(
		'https://api.stlouisfed.org/fred/series/observations?',
		'series_id=', series_id,
		'&api_key=', api_key,
		'&file_type=json',
		'&realtime_start=', x$start, '&realtime_end=', x$end,
		'&observation_start=', .obs_start, '&observation_end=', today,
		'&frequency=', .freq,
		'&aggregation_method=avg'
	))

	if (.verbose == T) message(obs_urls)

	get_obs = function(url) {
		response = req_perform(request(url))
		if (resp_is_error(response)) stop('Error')
		resp_body_json(response)$observations
	}

	raw_obs = list_c(map(
		obs_urls,
		\(url) insistently(get_obs, rate = rate_delay(30, 10), quiet = !.verbose)(url)
		))

	raw_obs %>%
		map(., as_tibble) %>%
		list_rbind %>%
		filter(., value != '.') %>%
		na.omit %>%
		transmute(
			.,
			# FRED weekly frequency is for week-ending value; switch to week starting value
			date = as_date(date) - days({if (.freq == 'w') 6 else 0}),
			vintage_date = as_date(realtime_start),
			varname = series_id,
			value = as.numeric(value)
		) %>%
		return(.)

}


#' Returns last available observations from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param series_id The FRED identifier of the time series to pull.
#' @param api_key A valid FRED API key.
#' @param .freq One of 'd', 'w', 'm', 'q'.
#' @param .obs_start The default start date of results to return.
#' @param .verbose If TRUE, echoes error messages.
#'
#' @return A data frame of data
#'
#' @description
#' This is an improvement of the original get_fred_data with simplified parameters.
#'  Note that since FRED returns weekly data in the form of week-ending dates, we subtract 7 days off the date to
#'  convert them into week beginning dates.
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date
#'
#' @export
get_fred_obs = function(series_id, api_key, .freq, .obs_start = '2000-01-01', .verbose = F)  {

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	obs_url = paste0(
		'https://api.stlouisfed.org/fred/series/observations?',
		'series_id=', series_id,
		'&api_key=', api_key,
		'&file_type=json',
		'&realtime_start=', today, '&realtime_end=', today,
		'&observation_start=', .obs_start, '&observation_end=', today,
		'&frequency=', .freq,
		'&aggregation_method=avg'
		)

	if (.verbose == T) message(obs_url)

	raw_obs = insistently(
		\(x) resp_body_json(req_perform(request(obs_url)))$observations,
		rate = rate_delay(30, 10),
		quiet = F
		)()

	raw_obs %>%
		map(., as_tibble) %>%
		list_rbind %>%
		filter(., value != '.') %>%
		na.omit %>%
		transmute(
			.,
			date = as_date(date) - days({if (.freq == 'w') 7 else 0}),
			varname = series_id,
			value = as.numeric(value)
		) %>%
		return(.)
}

# get_treasury_data = function() {
# 	treasury_data = c(
# 		'https://home.treasury.gov/system/files/276/yield-curve-rates-2011-2020.csv',
# 		paste0(
# 			'https://home.treasury.gov/resource-center/data-chart-center/interest-rates/daily-treasury-rates.csv/',
# 			2021:year(today()),
# 			'/all?type=daily_treasury_yield_curve&field_tdr_date_value=2023&page&_format=csv'
# 		)
# 	) %>%
# 		map(., .progress = T, \(x) read_csv(x, col_types = 'c')) %>%
# 		list_rbind(.) %>%
# 		pivot_longer(., cols = -c('Date'), names_to = 'varname', values_to = 'value') %>%
# 		separate(., col = 'varname', into = c('ttm_1', 'ttm_2'), sep = ' ') %>%
# 		mutate(
# 			.,
# 			varname = paste0('t', str_pad(ttm_1, 2, pad = '0'), ifelse(ttm_2 == 'Mo', 'm', 'y')),
# 			date = mdy(Date),
# 		) %>%
# 		transmute(
# 			.,
# 			vdate = date,
# 			varname,
# 			date,
# 			value
# 		) %>%
# 		filter(., !is.na(value))
#
# }
