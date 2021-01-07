#' Converts string dates of quarterly format (2012Q1, 2012Q2, ...) or monthly format to corresponding R Date objects.
#'
#' @param strdates A vector of string dates of quarterly or monthly format (YYYYQx or YYYYMxx).
#' @return A vector a R date objects
#' @export

strdateToDate = function(strdates) {

    strdates %>%
        {
            if (all(str_count(., 'Q') == 1))
                paste0(
                    str_sub(., 1, 4), '-',
                    str_pad(as.numeric(str_sub(., str_locate(., 'Q') + 1)) * 3 - 2, 2, pad = '0'), '-',
                    '01'
                ) %>%
                as.Date(.)


            else if (all(str_count(., 'M') == 1))
                paste0(
                    str_sub(., 1, 4), '-',
                    str_pad(as.numeric(str_sub(., str_locate(., 'M') + 1)), 2, pad = '0'), '-',
                    '01'
                ) %>%
                as.Date(.)

            else
                stop('Improper date format, dates must be in format YYYYQx or YYYYMxx')

        } %>%
        return(.)
}
