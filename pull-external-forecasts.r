#' @description Run this script on scheduler after close of business each day
#'
#'

# 1. Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sinkfile = file(file.path(DIR, 'logs', 'pull-external-forecasts-log.txt'), open = 'wt')
	sink(sinkfile, type = 'output')
	sink(sinkfile, type = 'message')
	message(
	'----------------------------------
	Run :', Sys.Date(), '
	----------------------------------
	')
}

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)

## Load Connection Info ----------------------------------------------------------
source(file.path(DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
	)
hist = list()
ext = list()

# Load Historical Data ----------------------------------------------------------

## 1. External Data ----------------------------------------------------------
local({

	fred_data =
		readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'variables') %>%
		purrr::transpose(.)	%>%
		purrr::keep(., ~ .$source == 'fred') %>%
		purrr::imap_dfr(., function(x, i) {
			message(str_glue('Pull {i}: {x$varname}'))
			get_fred_data(
					x$sckey,
					CONST$FRED_API_KEY,
					.freq = x$freq,
					.return_vintages = TRUE,
					.verbose = F
					) %>%
				transmute(
					.,
					sourcename = 'stl',
					varname = x$varname,
					form = 'base',
					freq = x$freq,
					date,
					vdate = vintage_date,
					value
					) %>%
				filter(., vdate >= as_date('2000-01-01'))
			})
})




# Load Data ----------------------------------------------------------

## 1. Atlanta Fed ----------------------------------------------------------
local({
	message('***** 1')
	paramsDf =
		tribble(
		  ~ varname, ~ fred_id,
		  'gdp', 'GDPNOW',
		  'pce', 'PCENOW',
		  'pdi', 'GDPINOW',
		  'govt', 'GOVNOW',
		  'ex', 'EXPORTSNOW',
		  'im', 'IMPORTSNOW'
		)

  # GDPNow
	df =
		paramsDf %>%
		purrr::transpose(.) %>%
		lapply(., function(x)
			get_fred_data(x$fred_id, CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
			dplyr::filter(., date >= .$vintage_date - months(3)) %>%
			dplyr::transmute(
				.,
				fcname = 'atl',
				varname = x$varname,
				form = 'd1',
				freq = 'q',
				date,
				vdate = vintage_date,
				value
				)
			) %>%
		dplyr::bind_rows(.)

	ext$atl <<- df
})


## 2. St. Louis Fed --------------------------------------------------------
local({
	message('***** 2')

	df =
		get_fred_data('STLENI', CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
		dplyr::filter(., date >= .$vintage_date - months(3)) %>%
		dplyr::transmute(
			.,
			fcname = 'stl',
			varname = 'gdp',
			form = 'd1',
			freq = 'q',
			date,
			vdate = vintage_date,
			value
			)

	ext$stl <<- df
})


## 3. New York Fed ---------------------------------------------------------
local({
	message('***** 3')

	file = file.path(tempdir(), 'nyf.xlsx')
    httr::GET(
        'https://www.newyorkfed.org/medialibrary/media/research/policy/nowcast/new-york-fed-staff-nowcast_data_2002-present.xlsx',
        httr::write_disk(file, overwrite = TRUE)
        )

    df =
        readxl::read_excel(file, sheet = 'Forecasts By Quarter', skip = 13) %>%
        dplyr::rename(., vdate = 1) %>%
        dplyr::mutate(., vdate = as_date(vdate)) %>%
        tidyr::pivot_longer(., -vdate, names_to = 'date', values_to = 'value') %>%
        na.omit(.) %>%
        dplyr::mutate(., date = from_pretty_date(date, 'q')) %>%
        dplyr::transmute(., fcname = 'nyf', varname = 'gdp', form = 'd1', freq = 'q', date, vdate, value)

  ext$nyf <<- df
})


## 4. Philadelphia Fed -----------------------------------------------------
local({
	message('***** 4')

    # Scrape vintage dates
    vintageDf =
    	httr::GET(paste0('https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
    		'survey-of-professional-forecasters/spf-release-dates.txt?'
    		)) %>%
	    httr::content(., as = 'text', encoding = 'UTF-8') %>%
	    str_sub(
	        .,
	        str_locate(., coll('1990 Q2'))[1], str_locate(., coll('*The 1990Q2'))[1] - 1
	        ) %>%
    	read_csv(., col_names = 'X1', col_types = 'c') %>%
    	.$X1 %>%
    	purrr::map_dfr(., function(x)
    		x %>%
    			str_squish(.) %>%
    			str_split(., ' ') %>%
    			.[[1]] %>%
    			{
    				if (length(.) == 4) tibble(X1 = .[1], X2 = .[2], X3 = .[3], X4 = .[4])
    				else if (length(.) == 3) tibble(X1 = NA, X2 = .[1], X3 = .[2], X4 = .[3])
    				else stop ('Error parsing data')
    				}
    		) %>%
    	tidyr::fill(., X1, .direction = 'down') %>%
    	dplyr::transmute(
            .,
            releasedate = from_pretty_date(paste0(X1, X2), 'q'),
            vdate = lubridate::mdy(str_replace_all(str_extract(X3, "[^\\s]+"), '[*]', ''))
        ) %>%
        # Don't include first date - weirdly has same vintage date as second date
        dplyr::filter(., releasedate >= as.Date('2000-01-01'))


    paramsDf =
        tribble(
          ~ varname, ~ spfname, ~ method,
          'gdp', 'RGDP', 'growth',
          'pce', 'RCONSUM', 'growth',
          'pdin', 'RNRESIN', 'growth',
          'pdir', 'RRESINV', 'growth',
          'govtf', 'RFEDGOV', 'growth',
          'govts', 'RSLGOV', 'growth',
          'ue', 'UNEMP', 'level',
          't03m', 'TBILL', 'level',
          't10y', 'TBOND', 'level',
          'houst', 'HOUSING', 'level',
          'inf', 'CORECPI', 'level'
          )


	df =
		lapply(c('level', 'growth'), function(m) {

			file = file.path(tempdir(), paste0('spf-', m, '.xlsx'))
	        httr::GET(
	            paste0(
	                'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/',
	                'survey-of-professional-forecasters/historical-data/median', m, '.xlsx?la=en'
	                ),
	                httr::write_disk(file, overwrite = TRUE)
	            )

	        paramsDf %>%
	        	dplyr::filter(., method == m) %>%
	        	purrr::transpose(.) %>%
		        lapply(., function(x)
		            readxl::read_excel(file, na = '#N/A', sheet = x$spfname) %>%
			            dplyr::select(
			            .,
			            c('YEAR', 'QUARTER', {
			              if (m == 'level') paste0(x$spfname, 2:6) else paste0('d', str_to_lower(x$spfname), 2:6)
			              })
			            ) %>%
			            dplyr::mutate(., releasedate = from_pretty_date(paste0(YEAR, 'Q', QUARTER), 'q')) %>%
			            dplyr::select(., -YEAR, -QUARTER) %>%
			            tidyr::pivot_longer(., -releasedate, names_to = 'fcPeriods') %>%
			            dplyr::mutate(., fcPeriods = as.numeric(str_sub(fcPeriods, -1)) - 2) %>%
			            dplyr::mutate(., date = add_with_rollback(releasedate, months(fcPeriods * 3))) %>%
			            na.omit(.) %>%
			            dplyr::inner_join(., vintageDf, by = 'releasedate') %>%
			            dplyr::transmute(
			            	.,
			            	fcname = 'spf', varname = x$varname, freq = 'q', form = 'd1', vdate, date, value
			            	)
		            ) %>%
		            dplyr::bind_rows(.) %>%
		            return(.)

	        }) %>%
	        dplyr::bind_rows(.)

	ext$spf <<- df
})



## 5. WSJ Economic Survey -----------------------------------------------------
# WSJ Survey Updated to Quarterly - see https://www.wsj.com/amp/articles/economic-forecasting-survey-archive-11617814998
local({
	message('***** 5')

    orgsDf =
        tibble(
          fcname = c(
          	'wsj', 'fnm', 'wfc', 'gsu',
          	'spg', 'ucl', 'gsc', 'mgs'
          	),
          fcfullname = c(
          	'WSJ Consensus', 'Fannie Mae', 'Wells Fargo & Co.', 'Georgia State University',
          	'S&P Global Ratings', 'UCLA Anderson Forecast', 'Goldman, Sachs & Co.', 'Morgan Stanley'
          	)
        )

    filePaths =
    	tribble(
    		~ vdate, ~ file,
    		'2021-04-11', 'wsjecon0421.xls',
				'2021-07-11', 'wsjecon0721.xls',
				'2021-10-17', 'wsjecon1021.xls'
				# October 17th next
    		) %>%
    	purrr::transpose(.)

    df =
        lapply(filePaths, function(x) {
            message(x$date)
            dest = file.path(tempdir(), 'wsj.xls')

            # A user-agent is required or garbage is returned
            httr::GET(
                paste0('https://online.wsj.com/public/resources/documents/', x$file),
                httr::write_disk(dest, overwrite = TRUE),
                httr::add_headers(
                	'Host' = 'online.wsj.com',
                	'User-Agent' = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
                	)
                )

            # Read first two lines to parse column names
            xlDf = suppressMessages(readxl::read_excel(dest, col_names = FALSE, .name_repair = 'unique'))

            # Create new column names
            xlRepair =
                xlDf %>%
                {tibble(colname = unlist(.[1, ]), date = unlist(.[2, ]))} %>%
                tidyr::fill(., colname) %>%
                dplyr::mutate(
                    .,
                    varname = str_to_lower(colname),
                    # https://stackoverflow.com/questions/4389644/regex-to-match-string-containing-two-names-in-any-order
                    # Repair varnames
                    varname = ifelse(str_detect(date, 'Organization'), 'fcfullname', varname),
                    varname = ifelse(str_detect(varname, '(?=.*gdp)(?=.*quarterly)'), 'gdp', varname),
                    varname = ifelse(str_detect(varname, '(?=.*fed)(?=.*funds)'), 'ffr', varname),
                    varname = ifelse(str_detect(varname, 'cpi'), 'inf', varname),
                    varname = ifelse(str_detect(varname, 'unemployment'), 'ue', varname),
                    # Keep varname
                    keep = varname %in% c('fcfullname', 'gdp', 'ffr', 'inf', 'ue')
                ) %>%
                dplyr::mutate(
                    .,
                    # Repair dates
                    date = str_replace_all(
                        date,
                        c(
                            'Fourth Quarter ' = '10 ', 'Third Quarter ' = '7 ',
                            'Second Quarter ' = '4 ', 'First Quarter ' = '1 ',
                            setNames(paste0(1:12, ' '), paste0(month.abb, ' ')),
                            setNames(paste0(1:12, ' '), paste0(month.name, ' '))
                            )
                        ),
                    date =
                        ifelse(
                            !date %in% c('Name:', 'Organization:') & keep == TRUE,
                            paste0(
                                str_sub(date, -4),
                                '-',
                                str_pad(str_squish(str_sub(date, 1, nchar(date) - 4)), 2, pad = '0'),
                                '-01'
                                ),
                            NA
                        ),
                    date = as_date(date)
                    )


            df =
                suppressMessages(readxl::read_excel(
                    dest,
                    col_names = paste0(xlRepair$varname, '_', xlRepair$date),
                    na = c('', ' ', '-', 'NA'),
                    skip = 2
                    )) %>%
                # Only keep columns selected as keep = TRUE in last step
                dplyr::select(
                    .,
                    xlRepair %>% dplyr::mutate(., index = 1:nrow(.)) %>% dplyr::filter(., keep == TRUE) %>% .$index
                    ) %>%
                dplyr::rename(., 'fcfullname' = 1) %>%
                # Bind WSJ row - select last vintage
                dplyr::mutate(., fcfullname = ifelse(fcfullname %in% month.name, 'WSJ Consensus', fcfullname)) %>%
                dplyr::filter(., fcfullname %in% orgsDf$fcfullname) %>%
                {
                    dplyr::bind_rows(
                        dplyr::filter(., !fcfullname %in% 'WSJ Consensus'),
                        dplyr::filter(., fcfullname %in% 'WSJ Consensus') %>% head(., 1)
                    )
                } %>%
                dplyr::mutate(., across(-fcfullname, as.numeric)) %>%
                tidyr::pivot_longer(., -fcfullname, names_sep = '_', names_to = c('varname', 'date')) %>%
                dplyr::mutate(., date = as_date(date)) %>%
                # Now split and fill in frequencies and quarterly data
                dplyr::group_by(., fcfullname, varname) %>%
                dplyr::group_split(.) %>%
            	purrr::keep(., function(z) nrow(dplyr::filter(z, !is.na(value))) > 0 ) %>% # Cull completely empty data frames
                purrr::map_dfr(., function(z)
                    tibble(
                        date = seq(from = min(na.omit(z)$date), to = max(na.omit(z)$date), by = '3 months')
                        ) %>%
                        dplyr::left_join(., z, by = 'date') %>%
                        dplyr::mutate(., value = zoo::na.approx(value)) %>%
                        dplyr::mutate(
                            .,
                            fcfullname = unique(z$fcfullname),
                            varname = unique(z$varname),
                            freq = 'q',
                            form = 'd1',
                            vdate = as_date(x$vdate)
                            )
                    ) %>%
                dplyr::right_join(., orgsDf, by = 'fcfullname') %>%
                dplyr::select(., -fcfullname)
        }) %>%
        dplyr::bind_rows(.) %>%
    	na.omit(.)

    ext$wsj <<- df
})

## Verify ------------------------------------------------------------------
local({
	ext$wsj %>% group_split(., varname) %>%
		setNames(., map(., ~.$varname[[1]])) %>%
		lapply(., function(x) tidyr::pivot_wider(x, id_cols = c('fcname', 'vdate'), names_from = 'date'))
})



## 6. CBO Forecasts --------------------------------------------------------
local({
	message('***** 6')

  urlDf =
      httr::GET('https://www.cbo.gov/data/budget-economic-data') %>%
      httr::content(., type = 'parsed') %>%
      xml2::read_html(.) %>%
      rvest::html_nodes('div .view-content') %>%
      .[[9]] %>%
      rvest::html_nodes(., 'a') %>%
      purrr::map_dfr(., function(x) tibble(date = rvest::html_text(x), url = rvest::html_attr(x, 'href'))) %>%
      dplyr::transmute(
          .,
          date =
              paste0(
                  str_sub(date, -4), '-',
                  str_pad(match(str_sub(date, 1, 3), month.abb), 2, pad = '0'),
                  '-01'
                  ),
          url
      ) %>%
      dplyr::mutate(., date = as.Date(date)) %>%
      dplyr::filter(., date >= as.Date('2020-01-01'))


  tempPath = file.path(tempdir(), 'cbo.xlsx')

  paramsDf =
    tribble(
      ~ varname, ~ cboCategory, ~ cboname, ~ cboUnits,
      'gdp', 'Output', 'Real GDP', 'Percentage change, annual rate',
      # 'pcepi', 'Prices', 'Price Index, Personal Consumption Expenditures (PCE)', 'Percentage change, annual rate',
      'inf', 'Prices', 'Consumer Price Index, All Urban Consumers (CPI-U)', 'Percentage change, annual rate',
      'oil', 'Prices', 'Price of Crude Oil, West Texas Intermediate (WTI)', 'Dollars per barrel',
      'ue', 'Labor', 'Unemployment Rate, Civilian, 16 Years or Older', 'Percent',
      'ffr', 'Interest Rates', 'Federal Funds Rate', 'Percent',
      't10y', 'Interest Rates', '10-Year Treasury Note', 'Percent',
      't03m', 'Interest Rates', '3-Month Treasury Bill', 'Percent',
      'pce', 'Components of GDP (Real)', 'Personal Consumption Expenditures', 'Percentage change, annual rate',
      'pdi', 'Components of GDP (Real)', 'Gross Private Domestic Investment', 'Percentage change, annual rate',
      'pdin', 'Components of GDP (Real)', 'Nonresidential fixed investment', 'Percentage change, annual rate',
      'pdir', 'Components of GDP (Real)', 'Residential fixed investment', 'Percentage change, annual rate',
      'govt', 'Components of GDP (Real)', 'Government Consumption Expenditures and Gross Investment',
    	'Percentage change, annual rate',
      'govtf', 'Components of GDP (Real)', 'Federal', 'Percentage change, annual rate',
      'govts', 'Components of GDP (Real)', 'State and local', 'Percentage change, annual rate',
      'ex', 'Components of GDP (Real)', 'Exports', 'Percentage change, annual rate',
      'im', 'Components of GDP (Real)', 'Imports', 'Percentage change, annual rate'
      )


  df =
    urlDf %>%
    purrr::transpose(.) %>%
    lapply(., function(x) {

      download.file(x$url, tempPath, mode = 'wb', quiet = TRUE)

      # Starts earlier form Jan 2019
      xl =
        suppressMessages(readxl::read_excel(
          tempPath,
          sheet = '1. Quarterly',
          skip = 6
          )) %>%
        dplyr::rename(., cboCategory = 1, cboname = 2, cboname2 = 3, cboUnits = 4) %>%
        dplyr::mutate(., cboname = ifelse(is.na(cboname), cboname2, cboname)) %>%
        dplyr::select(., -cboname2) %>%
        tidyr::fill(., cboCategory, .direction = 'down') %>%
        tidyr::fill(., cboname, .direction = 'down') %>%
        na.omit(.)

      xl %>%
        dplyr::inner_join(., paramsDf, by = c('cboCategory', 'cboname', 'cboUnits')) %>%
        dplyr::select(., -cboCategory, -cboname, -cboUnits) %>%
        tidyr::pivot_longer(-varname, names_to = 'date') %>%
        dplyr::mutate(., date = from_pretty_date(date, 'q')) %>%
        dplyr::filter(., date >= as.Date(x$date, origin = lubridate::origin)) %>%
        dplyr::mutate(., vdate = as.Date(x$date, origin = lubridate::origin))
      }) %>%
    dplyr::bind_rows(.) %>%
    dplyr::transmute(., fcname = 'cbo', varname, form = 'd1', freq = 'q', date, vdate, value)

  # Count number of forecasts per group
  # df %>% dplyr::group_by(vintageDate, varname) %>% dplyr::summarize(., n = n()) %>% View(.)

  ext$cbo <<- df
})


## 7. Cleveland Fed (Expected Inf) -----------------------------------------
local({
	message('***** 7')

  file = file.path(tempdir(), paste0('inf.xls'))

  download.file(
  	paste0(
  		'https://www.clevelandfed.org/en/our-research/indicators-and-data/~/media/content/our%20research/',
  		'indicators%20and%20data/inflation%20expectations/ie%20latest/ie%20xls.xls'
  		),
  	file,
  	mode = 'wb'
  	)

  df =
	readxl::read_excel(file, sheet = 'Expected Inflation') %>%
  	dplyr::rename(., vdate = 'Model Output Date') %>%
  	tidyr::pivot_longer(., -vdate, names_to = 'ttm', values_to = 'yield') %>%
  	dplyr::mutate(
  		.,
  		vdate = as.Date(vdate), ttm = as.numeric(str_replace(str_sub(ttm, 1, 2), ' ', '')) * 12
  		) %>%
  	# dplyr::filter(., vdate == max(vdate)) %>%
  	dplyr::filter(., vdate >= as_date('2015-01-01')) %>%
  	dplyr::group_split(., vdate) %>%
  	purrr::map_dfr(., function(x)
  		x %>%
		  	dplyr::right_join(., tibble(ttm = 1:360), by = 'ttm') %>%
		  	dplyr::arrange(., ttm) %>%
		  	dplyr::mutate(
		  		.,
		  		yield = zoo::na.spline(yield),
		  		vdate = unique(na.omit(vdate)),
		  		curDate = floor_date(vdate, 'months'),
		  		cumReturn = (1 + yield)^(ttm/12),
		  		yttmAheadCumReturn = dplyr::lead(cumReturn, 1)/cumReturn,
		  		yttmAheadAnnualizedYield = (yttmAheadCumReturn^(12/1) - 1) * 100,
		  		date = add_with_rollback(curDate, months(ttm - 1))
		  		)
  		) %>%
  	dplyr::transmute(
  		.,
		fcname = 'cle',
		varname = 'inf',
        form = 'd1',
		freq = 'm',
		date,
  		vdate,
  		value = yttmAheadAnnualizedYield,
  		) %>%
  	na.omit(.)

	df %>%
		filter(., month(vdate) %in% c(1, 6)) %>%
		ggplot(.) + geom_line(aes(x = date, y = value, color = as.factor(vdate)))

    ext$cle <<- df
})


## 8. CME ---------------------------------------------------------------------
local({
	message('***** 8')

	# First get from Quandl
	message('Starting Quandl data scrape...')
	df =
		lapply(1:24, function(j) {
			# message(j)
			read_csv(
				paste0(
					'https://www.quandl.com/api/v3/datasets/CHRIS/CME_FF', j,
					'.csv?api_key=', CONST$QUANDL_API_KEY
					),
				col_types = 'Ddddddddd'
				) %>%
				dplyr::transmute(., vdate = Date, settle = Settle, j = j) %>%
				dplyr::filter(., vdate >= as.Date('2010-01-01'))
			}) %>%
		dplyr::bind_rows(.) %>%
		dplyr::transmute(
			.,
			vdate,
			# Consider the forecasted period the vdate + j
			date =
				from_pretty_date(paste0(year(vdate), 'M', month(vdate)), 'm') %>%
				add_with_rollback(., months(j - 1), roll_to_first = TRUE),
			value = 100 - settle,
			varname = 'ffr',
			fcname = 'cme'
			)

	message('Completed Quandl data scrape')

	message('Starting CME data scrape...')
	cookieVal =
	    httr::GET(
            'https://www.cmegroup.com/',
            add_headers(c(
                'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
                'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Encoding' = 'gzip, deflate, br',
                'Accept-Language' ='en-US,en;q=0.5',
                'Cache-Control'='no-cache',
                'Connection'='keep-alive',
                'DNT' = '1'
                ))
            ) %>%
	    httr::cookies(.) %>% as_tibble(.) %>% dplyr::filter(., name == 'ak_bmsc') %>% .$value


	# Get CME Vintage Date
	lastTradeDate =
		httr::GET(
			paste0('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/305/G?quoteCodes=null&_='),
			add_headers(c(
				'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
				'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
				'Accept-Encoding' = 'gzip, deflate, br',
				'Accept-Language' ='en-US,en;q=0.5',
				'Cache-Control'='no-cache',
				'Connection'='keep-alive',
				'Cookie'=cookieVal,
				'DNT' = '1',
				'Host' = 'www.cmegroup.com'
			))
		) %>% content(., 'parsed') %>% .$tradeDate %>% lubridate::parse_date_time(., 'd-b-Y') %>% as_date(.)


	df2 =
		tribble(
			~ varname, ~ cmeId,
			'ffr', '305',
			'sofr', '8462',
			'sofr', '8463'
		) %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(var) {
			# message(var)
			content =
				httr::GET(
					paste0('https://www.cmegroup.com/CmeWS/mvc/Quotes/Future/', var$cmeId, '/G?quoteCodes=null&_='),
					# 3/30/21 - CME website now requires user-agent header
					add_headers(c(
						'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
						'Accept'= 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
						'Accept-Encoding' = 'gzip, deflate, br',
						'Accept-Language' ='en-US,en;q=0.5',
						'Cache-Control'='no-cache',
						'Connection'='keep-alive',
						'Cookie'=cookieVal,
						'DNT' = '1',
						'Host' = 'www.cmegroup.com'
						))
					) %>%
					httr::content(., as = 'parsed')

			content %>%
				.$quotes %>%
				purrr::map_dfr(., function(x) {
					if (x$priorSettle %in% c('0.00', '-')) return() # Whack bug in CME website
					tibble(
						date = lubridate::ymd(x$expirationDate),
						value = 100 - as.numeric(x$priorSettle),
						varname = var$varname
						)
					}) %>%
				return(.)
			}) %>%
		# Now average out so that there's only one value for each (varname, date) combo
		dplyr::group_by(varname, date) %>%
		dplyr::summarize(., value = mean(value), .groups = 'drop') %>%
		dplyr::arrange(., date) %>%
		# Get rid of forecasts for old observations
		dplyr::filter(., date >= lubridate::floor_date(Sys.Date(), 'month')) %>%
		# Assume vintagedate is the same date as the last Quandl obs
		dplyr::mutate(., vdate = lastTradeDate, fcname = 'cme') %>%
		dplyr::filter(., value != 100)


	message('Completed CME data scrape...')

	# Now combine, replacing df2 with df1 if necessary
	combinedDf =
		dplyr::full_join(df, df2, by = c('fcname', 'vdate', 'date', 'varname')) %>%
		# Use quandl data if available, otherwise use other data
		dplyr::mutate(., value = ifelse(!is.na(value.x), value.x, value.y)) %>%
		dplyr::select(., -value.x, -value.y)

	# Most data starts in 88-89, except j=12 which starts at 1994-01-04. Misc missing obs until 2006.
	# 	df %>%
	#   	tidyr::pivot_wider(., names_from = j, values_from = settle) %>%
	#     	dplyr::arrange(., date) %>% na.omit(.) %>% dplyr::group_by(year(date)) %>% dplyr::summarize(., n = n()) %>%
	# 		View(.)


	## Add monthly interpolation
	message('Adding monthly interpolation ...')

	finalDf =
		combinedDf %>%
		dplyr::group_by(vdate, varname) %>%
		dplyr::group_split(.) %>%
		lapply(., function(x) {
			x %>%
				# Join on missing obs dates
				dplyr::right_join(
					.,
					tibble(date = seq(from = .$date[[1]], to = tail(.$date, 1), by = '1 month')) %>%
						dplyr::mutate(n = 1:nrow(.)),
					by = 'date'
					) %>%
				dplyr::arrange(date) %>%
				dplyr::transmute(
					.,
					fcname = head(fcname, 1),
					varname = head(varname, 1),
					form = 'd1',
					freq = 'm',
					date = date,
					vdate = head(vdate, 1),
					value = zoo::na.spline(value)
					)
			}) %>%
		dplyr::bind_rows(.)

	ext$cme <<- finalDf
})




## 9. DNS - TDNS1, TDNS2, TDNS3, Treasury Yields, Spreads ---------------------
local({
	message('***** 9')

	variablesDf = readxl::read_excel(file.path(DIR, 'model-inputs', 'inputs.xlsx'), sheet = 'all-variables')

	fredRes =
		variablesDf %>%
		filter(., str_detect(fullname, 'Treasury Yield') | varname == 'ffr') %>%
		purrr::transpose(.) %>%
		purrr::map_dfr(., function(x) {

			message(x$sckey)
			# Get series data
			dataDf =
				get_fred_data(x$sckey, CONST$FRED_API_KEY, .return_vintages = TRUE) %>%
				dplyr::transmute(., varname = x$varname, date, vdate = vintage_date, value) %>%
				dplyr::filter(., date >= as.Date('2010-01-01'))
			dataDf
		})

	# Monthly aggregation & append EOM with current val
	fredResCat =
		fredRes %>%
		group_split(., varname) %>%
		# Add monthly values for current month
		map_dfr(., function(x)
			x %>%
				dplyr::mutate(., date = as.Date(paste0(year(date), '-', month(date), '-01'))) %>%
				dplyr::group_by(., varname, date) %>%
				dplyr::summarize(., value = mean(value), .groups = 'drop') %>%
				dplyr::mutate(., freq = 'm')
			)

	# Create tibble mapping tyield_3m to 3, tyield_1y to 12, etc.
	yieldCurveNamesMap =
		variablesDf %>%
		purrr::transpose(.) %>%
		map_chr(., ~.$varname) %>%
		unique(.) %>%
		purrr::keep(., ~ str_sub(., 1, 1) == 't' & str_length(.) == 4) %>%
		tibble(varname = .) %>%
		dplyr::mutate(., ttm = as.numeric(str_sub(varname, 2, 3)) * ifelse(str_sub(varname, 4, 4) == 'y', 12, 1))


	# Create training dataset from SPREAD from ffr - fitted on last 3 months
	trainDf =
		filter(fredResCat, varname %in% yieldCurveNamesMap$varname) %>%
		dplyr::select(., -freq) %>%
		dplyr::filter(., date >= add_with_rollback(Sys.Date(), months(-3))) %>%
		right_join(., yieldCurveNamesMap, by = 'varname') %>%
		left_join(., transmute(filter(fredResCat, varname == 'ffr'), date, ffr = value), by = 'date') %>%
		dplyr::mutate(., value = value - ffr) %>%
		dplyr::select(., -ffr)

	#' Calculate DNS fit
	#'
	#' @param df: (tibble) A tibble continuing columns obsDate, value, and ttm
	#' @param returnAll: (boolean) FALSE by default.
	#' If FALSE, will return only the MAPE (useful for optimization).
	#' Otherwise, will return a tibble containing fitted values, residuals, and the beta coefficients.
	#'
	#' @export
	getDnsFit = function(df, lambda, returnAll = FALSE) {
		df %>%
			dplyr::mutate(
				.,
				f1 = 1,
				f2 = (1 - exp(-1 * lambda * ttm))/(lambda * ttm),
				f3 = f2 - exp(-1 * lambda * ttm)
			) %>%
			dplyr::group_by(date) %>%
			dplyr::group_split(.) %>%
			lapply(., function(x) {
				reg = lm(value ~ f1 + f2 + f3 - 1, data = x)
				dplyr::bind_cols(x, fitted = fitted(reg)) %>%
					dplyr::mutate(., b1 = coef(reg)[['f1']], b2 = coef(reg)[['f2']], b3 = coef(reg)[['f3']]) %>%
					dplyr::mutate(., resid = value - fitted)
			}) %>%
			dplyr::bind_rows(.) %>%
			{
				if (returnAll == FALSE) dplyr::summarise(., mse = mean(abs(resid))) %>% .$mse
				else .
			} %>%
			return(.)
	}

	# Find MSE-minimizing lambda value
	optimLambda =
		optimize(
			getDnsFit,
			df = trainDf,
			returnAll = FALSE,
			interval = c(-1, 1),
			maximum = FALSE
		)$minimum

	# Store DNS coefficients
	dnsCoefs =
		getDnsFit(df = trainDf, optimLambda, returnAll = TRUE) %>%
		dplyr::filter(., date == max(date)) %>%
		dplyr::select(., b1, b2, b3) %>%
		head(., 1) %>%
		as.list(.)

	dnsFitChart =
		getDnsFit(df = trainDf, optimLambda, returnAll = TRUE) %>%
		dplyr::filter(., date == max(date)) %>%
		dplyr::arrange(., ttm) %>%
		ggplot(.) +
		geom_point(aes(x = ttm, y = value)) +
		geom_line(aes(x = ttm, y = fitted))


	print(dnsFitChart)


	# DIEBOLD LI FUNCTION SHOULD BE ffr + f1 + f2 () + f3()
	# Calculated TDNS1: TYield_10y
	# Calculated TDNS2: -1 * (t10y - t03m)
	# Calculated TDNS3: .3 * (2*t02y - t03m - t10y)
	# Keep these treasury yield forecasts as the external forecasts ->
	# note that later these will be "regenerated" in the baseline calculation,
	# may be off a bit due to calculation from TDNS, compare to

    # Monthly forecast up to 10 years
    # Get cumulative return starting from curDate
    fittedCurve =
        tibble(ttm = seq(1: 480)) %>%
        dplyr::mutate(., curDate = floor_date(Sys.Date(), 'months')) %>%
        dplyr::mutate(
          .,
          annualizedYield =
            dnsCoefs$b1 +
            dnsCoefs$b2 * (1-exp(-1 * optimLambda * ttm))/(optimLambda * ttm) +
            dnsCoefs$b3 * ((1-exp(-1 * optimLambda * ttm))/(optimLambda * ttm) - exp(-1 * optimLambda * ttm)),
          # Get cumulative yield
          cumReturn = (1 + annualizedYield/100)^(ttm/12)
          )

    # Test for 20 year forecast
    # fittedCurve %>% dplyr::mutate(., futNetYield = dplyr::lead(annualizedYield, 240)/cumReturn, futYield = (futNetYield^(12/240) - 1) * 100) %>% dplyr::filter(., ttm < 120) %>% ggplot(.) + geom_line(aes(x = ttm, y = futYield))

    # fittedCurve %>% dplyr::mutate(., futYield = (dplyr::lead(cumYield, 3)/cumYield - 1) * 100)

    # Iterate over "yttms" tyield_1m, tyield_3m, ..., etc.
    # and for each, iterate over the original "ttms" 1, 2, 3,
    # ..., 120 and for each forecast the cumulative return for the yttm period ahead.
    df0 =
    	yieldCurveNamesMap$ttm %>%
        lapply(., function(yttm)
            fittedCurve %>%
                dplyr::mutate(
                    .,
                    yttmAheadCumReturn = dplyr::lead(cumReturn, yttm)/cumReturn,
                    yttmAheadAnnualizedYield = (yttmAheadCumReturn^(12/yttm) - 1) * 100
                    ) %>%
                dplyr::filter(., ttm <= 120) %>%
                dplyr::mutate(., yttm = yttm) %>%
                dplyr::inner_join(., yieldCurveNamesMap, c('yttm' = 'ttm'))
            ) %>%
        dplyr::bind_rows(.) %>%
        dplyr::mutate(
            .,
            date = add_with_rollback(curDate, months(ttm - 1))
            ) %>%
        dplyr::transmute(
            .,
            fcname = 'dns',
            varname,
            date,
            form = 'd1',
            freq = 'm',
            vdate = Sys.Date(),
            value = yttmAheadAnnualizedYield
            )

    # Add ffr to forecasts
    df1 =
        df0 %>%
        dplyr::select(., varname, date, value) %>%
        dplyr::inner_join(
            .,
        	ext$cme %>%
                dplyr::filter(., vdate == max(vdate)) %>%
                dplyr::filter(., varname == 'ffr') %>%
                dplyr::transmute(., ffr = value, date),
            by = 'date'
        ) %>%
        dplyr::mutate(., value = value + ffr) %>%
        dplyr::transmute(
            .,
            fcname = 'dns',
            varname,
            date,
            form = 'd1',
            freq = 'm',
            vdate = Sys.Date(),
            value
            )

    # Calculate TDNS yield forecasts
    # Forecast vintage date should be bound to historical data vintage
    # date since reliant purely on historical data
    df2 =
        df0 %>%
        dplyr::select(., varname, date, value) %>%
        tidyr::pivot_wider(., names_from = 'varname') %>%
        dplyr::transmute(
            .,
            date,
            tdns1 = t10y,
            tdns2 = -1 * (t10y - t03m),
            tdns3 = .3 * (2 * t02y - t03m - t10y)
            ) %>%
        tidyr::pivot_longer(., -date, names_to = 'varname') %>%
        dplyr::transmute(., fcname = 'dns', varname, date, form = 'd1', freq = 'm', vdate = Sys.Date(), value)

    ext$dns <<- dplyr::bind_rows(df1, df2)
})


# Finalize  -------------------------------------------------

## Combine and Flatten -------------------------------------------------
local({
	message('***** 10')

	predFlat =
		ext %>%
		dplyr::bind_rows(.) %>%
		dplyr::transmute(., fcname, vdate, freq, form, varname, date, value)

	if (nrow(na.omit(predFlat)) != nrow(predFlat)) stop('Missing obs')

	flat <<- predFlat
})


## SQL: Variable Definitions -------------------------------------------------
local({
	
	if (RESET_SQL) {
		
		DBI::dbExecute(db, 'DROP TABLE IF EXISTS variable_definitions CASCADE')
		
		DBI::dbExecute(db, '
			CREATE TABLE variable_definitions (
				rundate DATE NOT NULL,
				varname VARCHAR(100) NOT NULL,
				fullname VARCHAR(100) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			);
		')
		
		DBI::dbExecute(db, 'CREATE INDEX external_forecast_names_ix_fctype ON external_forecast_names (fctype);')

	}
	
	
})


## Send External Forecast Meta Info SQL DB -------------------------------------------------
local({
	message('***** 11')

	if (RESET_SQL) {

		DBI::dbExecute(db, 'DROP TABLE IF EXISTS external_forecast_names CASCADE')

		# tstypes 'hist', 'forecast', 'nc'
		DBI::dbExecute(db, '
			CREATE TABLE external_forecast_names (
				tskey VARCHAR(3) CONSTRAINT external_forecast_names_pk PRIMARY KEY,
				fctype VARCHAR(255),
				shortname VARCHAR(100) NOT NULL,
				fullname VARCHAR(255) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			);
		')

		DBI::dbExecute(db, 'CREATE INDEX external_forecast_names_ix_fctype ON external_forecast_names (fctype);')
	}

	external_forecast_names = tribble(
		~ tskey, ~ fctype, ~ shortname, ~ fullname,
		# 1
		'atl', 'qual', 'Atlanta Fed', 'Atlanta Fed GDPNow Model',
		# 2
		'stl', 'qual', 'St. Louis Fed', 'St. Louis Fed Economic News Model',
		# 3
		'nyf', 'qual', 'NY Fed', 'New York Fed Staff Nowcast',
		# 4
		'spf', 'qual', 'Survey of Professional Forecasters', 'Survey of Professional Forecasters',
		# 5
		'wsj', 'qual', 'WSJ Consensus', 'Wall Street Journal Consensus Forecast',
		'fnm', 'qual', 'Fannie Mae', 'Fannie Mae Forecast',
		'wfc', 'qual', 'Wells Fargo', 'Wells Fargo Forecast',
		'gsu', 'qual', 'GSU', 'Georgia State University Forecast',
		'spg', 'qual', 'S&P', 'S&P Global Ratings',
		'ucl', 'qual', 'UCLA Anderson', 'UCLA Anderson Forecast',
		'gsc', 'qual', 'Goldman Sachs', 'Goldman Sachs Forecast',
		'mgs', 'qual', 'Morgan Stanley', 'Morgan Stanley',
		# 6
		'cbo', 'qual', 'CBO', 'Congressional Budget Office Projections',
		# 7
		'cle', 'fut', 'Futures-Implied Inflation Rates', 'Futures-Implied Expected Inflation Model',
		# 8
		'cme', 'fut', 'Futures-Implied Interest Rates', 'Futures-Implied Expected Benchmark Rates Model',
		# 9
		'dns', 'fut', 'Futures-Implied Treasury Yield', 'Futures-Implied Expected Treasury Yields Model'
	)

	flat %>% group_by(., fcname) %>% summarize(., n = n())

	create_insert_query(
		tsTypes,
		'external_forecast_names',
		str_squish('ON CONFLICT ON CONSTRAINT external_forecast_names_pk DO UPDATE
			    SET
			    fctype=EXCLUDED.fctype,
			    shortname=EXCLUDED.shortname,
			    fullname=EXCLUDED.fullname
			    ')
		) %>%
		DBI::dbSendQuery(db, .)

})

## Send Forecast Data SQL DB -------------------------------------------------
local({
	message('***** 11')

	if (RESET_SQL) {

		DBI::dbExecute(db, 'DROP TABLE IF EXISTS external_forecast_values CASCADE')
		DBI::dbExecute(db, '
			CREATE TABLE external_forecast_values (
				tskey CHAR(3) NOT NULL,
				vdate DATE NOT NULL,
				freq CHAR(1) NOT NULL,
				form VARCHAR(5) NOT NULL,
				varname VARCHAR(255) NOT NULL,
				date DATE NOT NULL,
				value NUMERIC(20, 4) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT external_forecast_values_pk PRIMARY KEY (tskey, vdate, freq, form, varname, date),
				CONSTRAINT external_forecast_values_fk FOREIGN KEY (tskey) REFERENCES external_forecast_names (tskey)
					ON DELETE CASCADE ON UPDATE CASCADE
				--CONSTRAINT ext_tsvalues_varname_fk FOREIGN KEY (varname) REFERENCES csm_params (varname)
				--	ON DELETE CASCADE ON UPDATE CASCADE
				);
			')

		DBI::dbExecute(db, '
			SELECT create_hypertable(
				relation => \'external_forecast_values\',
				time_column_name => \'vdate\'
				);
			')
	}

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		flat %>%
		transmute(., tskey = fcname, vdate, freq, form, varname, date, value) %>%
		filter(., vdate >= as_date('2010-01-01')) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_by(., split) %>%
		group_split(., .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'external_forecast_values',
				str_squish('ON CONFLICT (tskey, vdate, freq, form, varname, date) DO UPDATE
			    	SET value=EXCLUDED.value')
				) %>%
				DBI::dbExecute(db, .)
			) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}


	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL:')
	print(sum(unlist(sql_result)))

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_forecast_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)

})
