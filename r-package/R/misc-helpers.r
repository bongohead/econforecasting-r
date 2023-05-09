#' Convert a dataframe to a rowwise list of named lists
#'
#' @param df A dataframe to cast to a list.
#'
#' @examples \dontrun{
#'  microbenchmark::microbenchmark(
#' 	  purrr_pmap = pmap(economics, ~ list(...)),
#'    purrr_transpose = purrr::transpose(economics),
#' 	  purrr_list_transpose = list_transpose(as.list(economics), simplify = F, template = 1:nrow(economics), default = NA),
#' 	  base_split = lapply(split(economics, 1:nrow(economics)), as.list),
#' 	  dplyr_split = map(group_split(rowwise(economics)), as.list),
#' 	  times = 20
#'  )
#' }
#'
#' @import purrr
#'
#' @export
df_to_list = function(df) {
	return(pmap(df, ~ list(...)))
}
