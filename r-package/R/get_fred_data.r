#' Returns data from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param series_id The FRED identifier of the time series to pull.
#' @param api_key A valid FRED API key.
#' @param .freq One of 'd', 'm', 'q'. If NULL, returns highest available frequency.
#' @param .return_vintages If TRUE, returns all historic values ('vintages').
#' @param .vintage_date If .return_vintages = TRUE, .vintage_date can be set to only return the vintage for a single date.
#' @param .verbose If TRUE, echoes error messages.
#'
#' @return A data frame of forecasts.
#'
#' @import dplyr purrr httr
#' @export
get_fred_data = function(series_id, api_key, .freq = NULL, .return_vintages = FALSE, .vintage_date = NULL, .verbose = FALSE) {

	today = as_date(with_tz(now(), tz = 'America/Chicago'))

	url =
		paste0(
			'https://api.stlouisfed.org/fred/series/observations?',
			'series_id=', series_id,
			'&api_key=', api_key,
			'&file_type=json',
			'&realtime_start=', if (.return_vintages == TRUE & is.null(.vintage_date)) '2000-01-01' else if (.return_vintages == TRUE & !is.null(.vintage_date)) .vintage_date else today,
			'&realtime_end=', if (.return_vintages == TRUE & !is.null(.vintage_date)) .vintage_date else today,
			'&obs_start=', '2000-01-01',
			'&obs_end=', today,
			if(!is.null(.freq)) paste0('&frequency=', .freq) else '',
			'&aggregation_method=avg'
		)

	if (.verbose == TRUE) message(url)

	url %>%
		httr::RETRY('GET', url = ., times = 10) %>%
		httr::content(., as = 'parsed') %>%
		.$observations %>%
		purrr::map_dfr(., function(x) as_tibble(x)) %>%
		dplyr::filter(., value != '.') %>%
		na.omit(.) %>%
		dplyr::transmute(
			.,
			date = as_date(date),
			vintage_date = as_date(realtime_start),
			varname = series_id,
			value = as.numeric(value)
		) %>%
		{if(.return_vintages == TRUE) . else dplyr::select(., -vintage_date)} %>%
		return(.)
}
