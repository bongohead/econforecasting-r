#' Iterates through the columns of a time-series tibble with a "date" column
#'
#'
#' 
#' 
mapTsCols = function(df, fn = function(x) x) {
	newDfs = lapply(colnames(df) %>% .[. != 'date'] %>% setNames(., .), function(colname) dplyr::select(df, c('date', colname)))
	lapply(newDfs, fn)
}