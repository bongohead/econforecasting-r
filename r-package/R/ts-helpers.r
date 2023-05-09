#' Convert monthly or quarterly dates into nicely formatted dates
#'
#' @description
#' Converts monthly dates and quarterly dates into a pretty string format.
#'  2020-01-01 will be converted into 2020Q1 if quarterly, 2020M1 if monthly.
#'
#' @param x A vector of R dates.
#' @param frequency One of 'q', 'quarter', 'm', or 'month'.
#'
#' @import dplyr
#' @importFrom lubridate quarter month year
#'
#' @export
to_pretty_date = function(x, frequency) {

	if (frequency %in% c('q', 'quarter')) {
		paste0(year(x), 'Q', quarter(x))
	} else if (frequency %in% c('m', 'month')) {
		paste0(year(x), 'M', str_pad(month(x), 2, pad = '0'))
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
#' @import dplyr
#' @importFrom stringr str_sub str_locate str_pad
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


#' Guess the frequency of a time series data frame
#'
#' @param df A data frame.
#' @param date_col The column name of the data frame indicating the date.
#'
#' @examples
#' data(economics)
#' guess_frequency(economics, 'date')
#'
#' @import dplyr
#' @importFrom tibble as_tibble
#' @importFrom stringr str_glue
#'
#' @export
guess_frequency = function(df, date_col = 'date') {

	df = tryCatch(
		as_tibble(df),
		error = function(c) stop('"df" must an object coercible to a tibble.')
	)

	if (!is.character(date_col) || !date_col %in% colnames(df)) {
		stop(str_glue('"{date_col}" must be a column name of the data frame!'))
	}

	uniq_dates = sort(unique(df[[date_col]]))
	date_diffs = as.numeric(uniq_dates[2:length(uniq_dates)] - uniq_dates[1:(length(uniq_dates) - 1)])

	frequency = {
		if (all(date_diffs %in% 1)) 'day'
		else if (all(date_diffs %in% 7)) 'week'
		else if (all(date_diffs %in% 28:31)) 'month'
		else if (all(date_diffs %in% 90:92)) 'quarter'
		else if (all(date_diffs %in% 365:366)) 'year'
		else stop('No frequency provided and data not coercible to any frequency!')
	}


	return(frequency)
}


#' Fills missing date values in a time series
#'
#' @param df A data frame representing one or more time series of the same frequency.
#' @param date_col A character indicating the date column in the data frame.
#' @param id_cols A character vector of column names of "df"; this argument is only necessary if
#'  passing multiple time series into the function; the id_cols uniquely identify each time series.
#' @param frequency The frequency of the time series; one of "day", "week", "month", "quarter", "year",
#'  If left NULL, will attempt to auto-calculate.
#'
#' @examples
#' data(economics)
#' set.seed(12345)
#'
#' # Fill in missing data for a single time series
#' test_data = economics %>% select(date, pce) %>% sample_n(10)
#' fill_missing_dates(test_data, 'date', frequency = 'month')
#'
#' # Fill in missing data for multiple time series
#' test_data = economics %>%
#'  sample_n(100) %>%
#'  pivot_longer(., -date, names_to = 'varname')
#' fill_missing_dates(test_data, 'date', id_cols = 'varname', frequency = 'month')
#'
#' @import dplyr purrr
#' @importFrom tidyr as_tibble
#' @importFrom lubridate floor_date as_date
#' @importFrom stringr str_glue
#'
#' @export
fill_missing_dates = function(df, date_col = 'date', id_cols = NULL, frequency = NULL) {

	df = tryCatch(
		as_tibble(df),
		error = function(c) stop('"df" must an object coercible to a tibble.')
	)

	if (!is.character(date_col) || !date_col %in% colnames(df)) {
		stop(str_glue('"{date_col}" must be a column name of the data frame!'))
	}

	df = tryCatch(
		mutate(df, !!date_col := as_date(df[[date_col]])),
		error = function(c) stop('date_col must be a column castable to a date!')
		)

	if (!is.null(id_cols) && !all(id_cols %in% colnames(df))) {
		stop('id_cols must be a character vector of valid colnames!')
	}

	if (!is.null(frequency) && !frequency %in% c('day', 'week', 'month', 'quarter', 'year')) {
		stop('Argument "frequency" must be NULL or one of "day", "week", "month", "quarter", "year"')
	} else if (is.null(frequency)) {
		frequency = guess_frequency(df, date_col)
		message('No "frequency" provided, calculated to be: ', frequency)
	}


	df = mutate(df, !!date_col := lubridate::floor_date(df[[date_col]], unit = frequency))

	dfs = {if (!is.null(id_cols)) group_split(df, df[, id_cols]) else group_split(df, NULL)}

	df =
		lapply(dfs, function(x)
			tibble(
				!!date_col :=
					seq(
						from = min(x[[date_col]]),
						to = max(x[[date_col]]),
						by = frequency
					)
				) %>%
				purrr::reduce(id_cols, function(accum, id_col)
					mutate(accum, !!id_col := x[[id_col]][[1]]),
					.init = .
					) %>%
				left_join(x, by = c(date_col, id_cols))
		) %>%
		bind_rows()

	return(df)
}

