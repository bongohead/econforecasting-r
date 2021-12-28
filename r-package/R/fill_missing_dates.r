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


