#' Load a Matrix
#'
#' @param infile Path to the input file
#' @return A matrix of the infile
#' @export

getDataFred = function(seriesId, apiKey, .freq = NULL, .returnVintages = FALSE) {

    url =
        paste0(
            'https://api.stlouisfed.org/fred/series/observations?',
            'series_id=', seriesId,
            '&api_key=', apiKey,
            '&file_type=json',
            '&realtime_start=', if(.returnVintages == TRUE) '2000-01-01' else Sys.Date(),
            '&realtime_end=', Sys.Date(),
            '&obs_start=', '2000-01-01',
            '&obs_end=', Sys.Date(),
            if(!is.null(.freq)) '&freq=' else '',
            '&aggregation_method=avg'
        )

    # message(url)

    url %>%
        httr::GET(.) %>%
        httr::content(., as = 'parsed') %>%
        .$observations %>%
        purrr::map_dfr(., function(x) as_tibble(x)) %>%
        dplyr::filter(., value != '.') %>%
        na.omit(.) %>%
        dplyr::transmute(
            .,
            obsDate = as.Date(date),
            vintageDate = as.Date(realtime_start),
            varname = seriesId,
            value = as.numeric(value)
            ) %>%
        {if(.returnVintages == TRUE) . else dplyr::select(., -vintageDate)} %>%
        return(.)

}
