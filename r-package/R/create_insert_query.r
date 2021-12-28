#' Create a SQL INSERT query string.
#'
#' @param df A data frame to insert into SQL.
#' @param tblname The name of the SQL table.
#' @param .append Any additional characters to append to the end of the query.
#'
#' @return A character string representing a SQL query.
#'
#' @export
create_insert_query = function(df, tblname, .append = '') {

    paste0(
        'INSERT INTO ', tblname, ' (', paste0(colnames(df), collapse = ','), ')\n',
        'VALUES\n',
        df %>%
            dplyr::mutate_if(is.Date, as.character) %>%
            # dplyr::mutate_if(is.character, function(x) paste0("'", x, "'")) %>%
            dplyr::mutate_if(is.character, function(x) dbQuoteString(ANSI(), x)) %>%
            dplyr::mutate_if(is.numeric, function(x) dbQuoteString(ANSI(), as.character(x))) %>%

            tidyr::unite(., 'x', sep = ',') %>%
            dplyr::mutate(., x = paste0('(', x, ')')) %>%
            .$x %>%
            paste0(., collapse = ', '), '\n',
        .append, ';'
        ) %>%
        return(.)
}
