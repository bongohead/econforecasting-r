#' Scrape data from FRED.
#'
#' @param seriesId (string) FRED ID of economic variable
#' @param apiKey (string) User's FRED API Key
#' @param .freq (string) One of 'd', 'm', 'q'. If NULL, returns highest available frequency.
#' @param .returnVintages (boolean) If TRUE, returns all historic forecast values ('vintages').
#' @return A data frame of forecasts
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
            if(!is.null(.freq)) paste0('&frequency=', .freq) else '',
            '&aggregation_method=avg'
        )

    # message(url)

    url %>%
        httr::RETRY('GET', url = ., times = 10) %>%
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
