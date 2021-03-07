#' Returns data from St. Louis Federal Reserve Economic Database (FRED)
#'
#' @param seriesId (string) FRED ID of economic variable
#' @param apiKey (string) User's FRED API Key
#' @param .freq (string) One of 'd', 'm', 'q'. If NULL, returns highest available frequency.
#' @param .returnVintages (boolean) If TRUE, returns all historic forecast values ('vintages').
#' @param .vintageDate (date) If .returnVintages = TRUE, .vintageDate can be set to only return the vintage for a single date
#' @param .verbose (boolean) If TRUE, returns boolean
#' @return A data frame of forecasts
#' @export

getDataFred = function(seriesId, apiKey, .freq = NULL, .returnVintages = FALSE, .vintageDate = NULL, .verbose = FALSE) {

    url =
        paste0(
            'https://api.stlouisfed.org/fred/series/observations?',
            'series_id=', seriesId,
            '&api_key=', apiKey,
            '&file_type=json',
            '&realtime_start=', if (.returnVintages == TRUE & is.null(.vintageDate)) '2000-01-01' else if (.returnVintages == TRUE & !is.null(.vintageDate)) .vintageDate else Sys.Date(),
            '&realtime_end=', if (.returnVintages == TRUE & !is.null(.vintageDate)) .vintageDate else Sys.Date(),
            '&obs_start=', '2000-01-01',
            '&obs_end=', Sys.Date(),
            if(!is.null(.freq)) paste0('&frequency=', .freq) else '',
            '&aggregation_method=avg'
        )

    if (.verbose == TRUE) message(url)

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
