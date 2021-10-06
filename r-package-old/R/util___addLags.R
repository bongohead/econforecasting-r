#' Add lagged versions to all numeric columns in a data frame
#'
#' @param df (tibble) Data frame with data to lag
#' @param lag (integer) Maximum number of lags to return
#' @param .zero (boolean) Return original non-lagged data-frame? TRUE by default.
addLags = function(df, lag, .zero = TRUE) {
    lapply(1:lag, function(l)
        dplyr::transmute(df, across(where(function(x) is.numeric(x)), function(x) dplyr::lag(x, l))) %>%
            setNames(., paste0(colnames(.), '.l', l))
    ) %>%
        {
            if (.zero == TRUE) dplyr::bind_cols(df, .)
            else dplyr::bind_cols(dplyr::select(df, where(function(x) !is.numeric(x))), .)
        }
}
