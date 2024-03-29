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
get_fred_data = function(series_id, api_key, .freq = NULL, .return_vintages = F, .vintage_date = NULL, .obs_start = '2000-01-01', .verbose = F) {

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	url = paste0(
		'https://api.stlouisfed.org/fred/series/observations?',
		'series_id=', series_id,
		'&api_key=', api_key,
		'&file_type=json',
		'&realtime_start=', if (.return_vintages == T & is.null(.vintage_date)) .obs_start else if (.return_vintages == T & !is.null(.vintage_date)) .vintage_date else today,
		'&realtime_end=', if (.return_vintages == T & !is.null(.vintage_date)) .vintage_date else today,
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




#' Returns last available observations from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param pull_ids A vector of FRED series IDs to pull from such as `c(id1, id2, ...)`;
#'  alternatively, a list of FRED series IDs and corresponding frequencies: `list(c(id1, d), (id2, w), ..)`.
#' @param api_key A valid FRED API key.
#' @param .obs_start The default start date of results to return.
#' @param .verbose If TRUE, echoes error messages.
#'
#' @return A data frame of data with each observation corresponding to a unique date/series_id/value.
#'
#' @description
#' Get latest observations for multiple data series from FRED.
#'  Note that since FRED returns weekly data in the form of week-ending dates, we subtract 7 days off the date to
#'  convert them into week beginning dates.
#'
#' @import dplyr purrr httr2
#' @importFrom lubridate with_tz now as_date is.Date
#' @importFrom purrr every is_list is_scalar_character is_character is_scalar_logical
#'
#' @examples \dontrun{
#'  get_fred_obs_async(list(c('GDP', 'a'), c('PCE', 'a')), api_key)
#'  get_fred_obs_async(c('GDP', 'PCE'), api_key)
#' }
#'
#' @export
get_fred_obs_async = function(pull_ids, api_key, .obs_start = '2000-01-01', .verbose = F)  {

	stopifnot(
		(is_list(pull_ids) & every(pull_ids, \(x) length(x) == 2 && is_character(x))) || is_character(pull_ids),
		is.Date(.obs_start) || is_scalar_character(.obs_start),
		is_scalar_logical(.verbose)
		)

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	# Build hashmap of inputs per series
	reqs_map = {
		if (is_list(pull_ids)) map(pull_ids, \(x) list(series_id = x[1], freq = x[2]))
		else map(pull_ids, \(x) list(series_id = x, freq = NA))
		}

	requests = map(reqs_map, \(x)
		request(paste0(
			'https://api.stlouisfed.org/fred/series/observations?',
			'series_id=', x$series_id,
			'&api_key=', api_key,
			'&file_type=json',
			'&realtime_start=', today, '&realtime_end=', today,
			'&observation_start=', .obs_start, '&observation_end=', today,
			{if (is.na(x$freq)) '' else paste0('&frequency=', x$freq)},
			'&aggregation_method=avg'
			)) %>%
			req_timeout(., 8000)
	)

	http_responses = send_async_requests(requests, .chunk_size = 1, .max_retries = 10, .verbose = .verbose)
	parsed_results = imap(http_responses, \(r, i)
	  resp_body_json(r)$observations %>%
	  	map(., as_tibble) %>%
	  	list_rbind %>%
	  	filter(., value != '.') %>%
	  	na.omit %>%
	  	transmute(
	  		.,
	  		date = as_date(date) - days({if (!is.na(reqs_map[[i]]$freq) && reqs_map[[i]]$freq == 'w') 7 else 0}),
	  		series_id = reqs_map[[i]]$series_id,
	  		value = as.numeric(value),
	  		freq = {if (!is.na(reqs_map[[i]]$freq)) reqs_map[[i]]$freq else NA}
	  	)
	)


	return(list_rbind(parsed_results))
}

