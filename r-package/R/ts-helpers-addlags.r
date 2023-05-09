#' Add lagged values to columns in a data frame
#'
#' @description This function adds lagged columns with names lags appended to the column name, i.e.
#'  col1.l1, col1.l2, col2.l1, col2.l2, etc.
#'
#' @param df A data frame representing a time series.
#' @param date_col The column name of the data frame indicating the date.
#' @param cols Names of columns to calculate the lag for. If NULL, will calculate the lag for all non-date columns.
#' @param max_lag Maximum number of lags to return.
#'
#' @examples
#' data(economics)
#' add_lagged_columns(economics, date_col = 'date', max_lag = 2)
#'
#' add_lagged_columns(economics, date_col = 'date', cols = c('pce', 'pop'), max_lag = 2)
#'
#' @import dplyr
#' @importFrom stringr str_glue
#' @importFrom tibble as_tibble
#'
#' @export
add_lagged_columns = function(df, date_col = 'date', cols = NULL, max_lag = 1) {

	df = tryCatch(
		as_tibble(df),
		error = function(c) stop('"df" must an object coercible to a tibble.')
	)
	if (!is.character(date_col) || !date_col %in% colnames(df)) {
		stop(str_glue('"{date_col}" must be a column name of the data frame!'))
	}
	df = arrange(df, date_col)


	if (is.null(cols)) {

		lapply(1:max_lag, function(l)
			transmute(df, across(where(function(x) is.numeric(x)), function(x) dplyr::lag(x, l))) %>%
				setNames(., paste0(colnames(.), '.l', l))
			) %>%
			bind_cols(select(df, where(function(x) !is.numeric(x))), .)
			
	} else {

		lapply(1:max_lag, function(l)
			transmute(df, across(cols, function(x) dplyr::lag(x, l))) %>%
				setNames(., paste0(colnames(.), '.l', l))
			) %>%
			bind_cols(select(df, all_of(c(date_col, cols))), .)
	}
}
