#' Equivalent to dplyr::group_split(.) with names
#'
#' @param df: A data frame or object coercible to a data frame.
#' @export


namedSplit = function(df) {

    df %>%
        dplyr::group_split(.) %>%
        set_names(unlist(group_keys(df)))

}
