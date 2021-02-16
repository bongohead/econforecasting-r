#' Conducts common time series transformations - differencing, log-differencing, moving-averages, and lagged moving averages
#'
#' @param x (numeric vector) A vector of data to be transformed
#' @param .l (integer) Number of laggs for taking a difference
#' @param .length (integer) Length of lags for taking moving average
#' @export


diff = function(x, .l) {
	x - dplyr::lag(x, .l)
}


diff1 = function(x) {
	diff(x, .l = 1)
}


diff2 = function(x) {
	diff(x, .l = 2)
}


dlog = function(x) {
	log(x/dplyr::lag(x, 1))
}


ma = function(x, .length) {
	x %>%
		tibble(.) %>%
		addLags(., .length - 1, .zero = TRUE) %>%
		dplyr::rowwise(.) %>%
		dplyr::mutate(mean = mean(.)) %>%
		.$mean
}

lma = function(x, .length) {
	x %>%
		tibble(.) %>%
		addLags(., .length, .zero = FALSE) %>%
		dplyr::rowwise(.) %>%
		dplyr::mutate(., mean = mean(c_across(everything()))) %>%
		.$mean
}


# tibble(thisEl = seq(1, 1.05, .01)) %>% purrr::transpose(.) %>% purrr::accumulate(., function(accum, x) list(thisEl = accum$thisEl + x$thisEl))