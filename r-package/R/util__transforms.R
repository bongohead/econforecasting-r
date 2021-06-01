#' Conducts common time series transformations - differencing, log-differencing, moving-averages, and lagged moving averages
#'
#' @param x (numeric vector) A vector of data to be transformed
#' @param .l (integer) Number of lags for taking a difference
#' @param .length (integer) Length of lags for taking moving average
#' @param .h (integer vector) Non-transformed vector of historical values for reversing transformations
#' @export


diff = function(x, .l) {
	x - dplyr::lag(x, .l)
}

undiff = function(x, .l, .h) {
    if (length(.h) < .l) stop('Length of .p must be greater than or equal to .l')
    purrr::accumulate(x, function(accum, z) accum + z, .init = .h[length(.h) - .l + 1]) %>% .[2:length(.)]
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

undlog = function(x, .h) {
    undiff(x = x, .l = 1, .h = log(.h)) %>%
        exp(.)
}


apchg = function(x, .periods = 12) {
    ((x/dplyr::lag(x, 1))^.periods - 1) * 100
}

unapchg = function(x, .periods = 12, .h) {
    purrr::accumulate((x/100 + 1)^(1/.periods), function(accum, z) accum * z, .init = .h)
}


pchg = function(x) {
    (x/dplyr::lag(x, 1) - 1) * 100
}


unpchg = function(x, .h) {
    purrr::accumulate((x/100 + 1), function(accum, z) accum * z, .init = .h)
}




ma = function(x, .length) {
	x %>%
		tibble(.) %>%
		addLags(., .length - 1, .zero = TRUE) %>%
		dplyr::rowwise(.) %>%
        dplyr::mutate(., m = rowMeans(across(where(is.numeric)))) %>%
        .$m
}

ma2 = function(x) {
    ma(x, 2)
}


lma = function(x, .length) {
	x %>%
		tibble(.) %>%
		addLags(., .length, .zero = FALSE) %>%
		dplyr::rowwise(.) %>%
		dplyr::mutate(., mean = mean(c_across(everything()))) %>%
		.$mean
}

lma2 = function(x) {
    lma(x, 2)
}


# tibble(thisEl = seq(1, 1.05, .01)) %>% purrr::transpose(.) %>% purrr::accumulate(., function(accum, x) list(thisEl = accum$thisEl + x$thisEl))
