#' Create a SQL INSERT query string.
#'
#' @param df A data frame to insert into SQL.
#' @param tblname The name of the SQl table.
#' @param .append (string) Any additional characters to append to the end of the query.
#' @return A character string representing a SQL query.
#' @export

createInsertQuery = function(df, tblname, .append = '') {
    # Note 2021-02-03: Using mutates significantly speeds up operation over apply iteration

    paste0(
        'INSERT INTO ', tblname, ' (', paste0(colnames(df), collapse = ','), ')\n',
        'VALUES\n',
        df %>%
            dplyr::mutate_if(is.Date, as.character) %>%
            # dplyr::mutate_if(is.character, function(x) paste0("'", x, "'")) %>%
            dplyr::mutate_if(is.character, function(x) dbQuoteString(ANSI(), x)) %>%
            tidyr::unite(., 'x', sep = ',') %>%
            dplyr::mutate(., x = paste0('(', x, ')')) %>%
            .$x %>%
            # purrr::transpose(.) %>%
            # lapply(., function(x)
            #     paste0(sapply(x, function(y) if (is.character(y)) paste0("'", y, "'") else y), collapse = ',') %>%
            #         paste0('(', ., ')')
            # ) %>%
            paste0(., collapse = ', '), '\n',
        .append, ';'
        ) %>%
        return(.)
}
