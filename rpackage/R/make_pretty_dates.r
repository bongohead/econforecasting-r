#' Convert monthly or quarterly dates into nicely formatted dates
#'
#' @description
#' Converts monthly dates and quarterly dates into a pretty string format.
#'  2020-01-01 will be converted into 2020Q1 if quarterly, 2020M1 if monthly.
#'
#' @param x A vector of R dates.
#' @param frequency One of 'q', 'quarter', 'm', or 'month'.
#'
#' @export
to_pretty_date = function(x, frequency) {

	if (frequency %in% c('q', 'quarter')) {
		paste0(lubridate::year(x), 'Q', lubridate::quarter(x))
	} else if (frequency %in% c('m', 'month')) {
		paste0(lubridate::year(x), 'M', str_pad(lubridate::month(x), 2, pad = '0'))
	} else {
		stop('Argument "frequency" must be one of "q", "quarter", "m", "month".')
	}
}

#' Convert formatted dates make by make_pretty_date by to regular date objects.
#'
#' @param A vector of formatted dates.
#' @param frequency One of 'q', 'quarter', 'm', or 'month'.
#'
#' @rdname to_pretty_date
#'
#' @export
from_pretty_date = function(x, frequency) {

	if (frequency %in% c('q', 'quarter')) {
		paste0(
			str_sub(x, 1, 4), '-',
			str_pad(as.numeric(str_sub(x, str_locate(x, 'Q')[, 'start'] + 1)) * 3 - 2, 2, pad = '0'), '-',
			'01'
			) %>%
			as_date(.)
	} else if (frequency %in% c('m', 'month')) {
		paste0(
			str_sub(x, 1, 4), '-',
			str_pad(as.numeric(str_sub(x, str_locate(x, 'M')[, 'start'] + 1, nchar(x))), 2, pad = '0'), '-',
			'01'
			) %>%
			as_date(.)
	} else {
		stop('Argument "frequency" must be one of "q", "quarter", "m", "month".')
	}
}
