#' Convert a dataframe to a rowwise list of named lists
#'
#' @param df A dataframe to cast to a list.
#'
#' @examples \dontrun {
#' microbenchmark::microbenchmark(
#'	 purrr_pmap = pmap(economics, ~ list(...)),
#' 	 purrr_transpose = purrr::transpose(economics),
#' 	 base_split = lapply(split(economics, 1:nrow(economics)), as.list),
#' 	 dplyr_slpit = map(group_split(rowwise(economics)), as.list),
#' 	 times = 20
#' )
#' }
#'
#' @export
df_to_list = function(df) {
	return(pmap(df, ~ list(...)))
}
