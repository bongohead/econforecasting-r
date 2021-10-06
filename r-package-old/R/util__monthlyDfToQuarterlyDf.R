#' Aggregate a monthly data frame into a quarterly data frame
#'
#' @param df: A data frame with a date column and a value column
#' @param .requireAll: Boolean, if TRUE then will only return quarters for which there are 3 months of data
monthlyDfToQuarterlyDf = function(df, .requireAll = TRUE) {
	df %>%
		dplyr::mutate(., date = strdateToDate(paste0(year(date), 'Q', quarter(date)))) %>%
		dplyr::group_by(., date) %>%
		dplyr::summarize(., value = mean(value), .groups = 'drop', n = n()) %>%
		# Only keep if all 3 monthly data exists (or imputed by previous chunk)
		{
			if (.requireAll == TRUE) dplyr::filter(., n == 3)
			else .
			} %>%
		dplyr::select(., -n)
}
