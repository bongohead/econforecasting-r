#' Guess the frequency of a time series data frame
#'
#' @param df A data frame.
#' @param date_col The column name of the data frame indicating the date.
#'
#' @examples
#' data(economics)
#' guess_frequency(economics, 'date')
#'
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
