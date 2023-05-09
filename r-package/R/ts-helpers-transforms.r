#' Conducts differencing and reverse-differencing of a time series
#'
#' @param x A numeric vector of data to be transformed.
#' @param .l An integer representing the number of lags.
#' @param .h A non-transformed vector of historical values for reversing transformations.
#'
#' @examples
#' data(economics)
#' dat = mutate(economics, pop = diff(pop, 1))
#' # Reverse the transformation if you have the last historical date
#' original = mutate(dat[2:nrow(dat), ], pop = undiff(pop, 1, economics$pce[[1]]))
#'
#' # If multiple differencing is used, a vector of historical data must be passed
#' dat = mutate(economics, pop = diff(pop, 2))
#' original = mutate(dat, pop = economics$pce[[1]])
#'
#' @import dplyr purrr
#' @export
diff = function(x, .l) {
	x - dplyr::lag(x, .l)
}

#' @rdname diff
#' @export
undiff = function(x, .l, .h) {
	if (length(.h) < .l) stop('Length of .p must be greater than or equal to .l')
	purrr::accumulate(x, function(accum, z)
		accum + z,
		.init = .h[length(.h) - .l + 1]
		) %>% .[2:length(.)]
}

#' @rdname diff
#' @export
diff1 = function(x) {
	diff(x, .l = 1)
}

#' @rdname diff
#' @export
diff2 = function(x) {
	diff(x, .l = 2)
}


#' Conducts log-differencing and reverse log-differencing of a time series
#'
#' @param x A numeric vector of data to be transformed.
#' @param .h A non-transformed vector of historical values for reversing transformations.
#'
#' @examples
#' data(economics)
#' dat = mutate(economics, popgrowth = dlog(pop))
#' # Reverse the transformation if you have the last historical date
#' original = mutate(dat[2:nrow(dat), ], poporig = undlog(popgrowth, economics$pop[[1]]))
#'
#' @export
dlog = function(x) {
	log(x/dplyr::lag(x, 1))
}

#' @rdname dlog
#' @export
undlog = function(x, .h) {
	undiff(x = x, .l = 1, .h = log(.h)) %>%
		exp(.)
}



#' Conducts percent-change calculations of a time series
#'
#' @description
#' Provides functions to calculate the annualized percent changes or the percent change from the
#'  previous period, and reverse the transformation.
#'
#' @param x A numeric vector of data to be transformed.
#' @param .periods An integer indicating the per-year frequency for calculating annualized percent changes.
#' @param .h A non-transformed vector of historical values for reversing transformations.
#'
#' @examples
#' data(economics)
#' dat = mutate(economics, popgrowth = apchg(pop, 12)) # Use 12 for monthly frequency
#' # Reverse the transformation if you have the last historical date
#' original = mutate(dat[2:nrow(dat), ], poporig = unapchg(popgrowth, 12, economics$pop[[1]]))
#'
#' @export
apchg = function(x, .periods = 12) {
	((x/dplyr::lag(x, 1))^.periods - 1) * 100
}

#' @rdname apchg
#' @export
unapchg = function(x, .periods = 12, .h) {
	purrr::accumulate((x/100 + 1)^(1/.periods), function(accum, z) accum * z, .init = .h) %>%
		.[2:length(.)]
}

#' @rdname apchg
#' @export
pchg = function(x) {
	(x/dplyr::lag(x, 1) - 1) * 100
}

#' @rdname apchg
#' @export
unpchg = function(x, .h) {
	purrr::accumulate((x/100 + 1), function(accum, z) accum * z, .init = .h) %>% .[2:length(.)]
}



#' Conducts moving-average calculations of a time series
#'
#' @description
#' Provides functions to calculate the annualized percent changes or the percent change from the
#'  previous period, and reverse the transformation.
#'
#' @param x A numeric vector of data to be transformed.
#' @param .length An integer indicating the moving-average window length.
#' @param .h A non-transformed vector of historical values for reversing transformations.
#'
#' @examples
#' data(economics)
#' # Calculate 4-quarter moving average
#' dat = mutate(economics, popgrowth = ma(pop, 4))
#'
#' # Calculate 4-quarter weighted moving average
#' dat = mutate(economics, popgrowth = wma(pop, 4))
#'
#' # Calculate 4-quarter lagged moving average
#' dat = mutate(economics, popgrowth = lma(pop, 4))
#'
#' @export
ma = function(x, .length) {
	data.frame(x) %>%
		cbind(., lapply(1:(.length - 1), function(.l) matrix(lag(x, .l)))) %>%
		rowMeans(.)
}

#' @rdname ma
#' @export
ma2 = function(x) {
	ma(x, 2)
}

#' @rdname ma
#' @export
ma4 = function(x) {
	ma(x, 4)
}

#' @rdname ma
#' @export
wma = function(x, .length) {
	x %>%
		{lapply(0:(.length - 1), function(l) tibble(value = lag(., l), lag = l, dateidx = 1:length(.)))} %>%
		dplyr::bind_rows(.) %>%
		dplyr::mutate(., weight = 1/(lag + 1)/sum(1/(1:(.length))), wval = value * weight) %>%
		dplyr::group_by(., dateidx) %>%
		dplyr::summarize(., sumwval = sum(wval)) %>%
		dplyr::arrange(., dateidx) %>%
		.$sumwval
}

#' @rdname ma
#' @export
wma2 = function(x) {
	wma(x, 2)
}

#' @rdname ma
#' @export
wma4 = function(x) {
	wma(x, 4)
}

#' @rdname ma
#' @export
get_wma_weights = function(length) {
	lapply(0:(length - 1), function(l) tibble(lag = l)) %>%
		dplyr::bind_rows(.) %>%
		dplyr::mutate(., weight = 1/(lag + 1)/sum(1/(1:(length)))) %>%
		.$weight
}

#' @rdname ma
#' @export
lma = function(x, .length) {
	data.frame(x) %>%
		{cbind(., lapply(1:(.length), function(.l) matrix(lag(x, .l))))} %>%
		.[, 2:ncol(.)] %>%
		rowMeans(.)
}

#' @rdname ma
#' @export
lma2 = function(x) {
	lma(x, 2)
}
