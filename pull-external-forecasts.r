library(econforecasting)

local({
  
  ##### GDP #####
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

	res$atl <<- df
})


## St. Louis Fed
local({
 
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

	res$stl <<- df
})


## New York Fed
local({
  
    file = file.path(DL_DIR, 'nyf.xlsx')
    httr::GET(
        'https://www.newyorkfed.org/medialibrary/media/research/policy/nowcast/new-york-fed-staff-nowcast_data_2002-present.xlsx?la=en',
        httr::write_disk(file, overwrite = TRUE)
        )
  
    df =
        readxl::read_excel(file, sheet = 'Forecasts By Quarter', skip = 13) %>%
        dplyr::rename(., vintageDate = 1) %>%
        dplyr::mutate(., vintageDate = as.Date(vintageDate)) %>%
        tidyr::pivot_longer(., -vintageDate, names_to = 'obsDate', values_to = 'value') %>%
        na.omit(.) %>%
        dplyr::mutate(., obsDate = econforecasting::strdateToDate(obsDate)) %>%
        dplyr::transmute(., fcname = 'nyf', varname = 'gdp', form = 'd1', freq = 'q', date = obsDate, vdate = vintageDate, value)

  m$ext$sources$nyf <<- df
})
```

## Philadelphia Fed
```{r}
local({
  
    # Scrape vintage dates
    vintageDf =
    	httr::GET('https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/survey-of-professional-forecasters/spf-release-dates.txt?la=en&hash=B0031909EE9FFE77B26E57AC5FB39899') %>%
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
            releaseDate = econforecasting::strdateToDate(paste0(X1, X2)),
            vintageDate = lubridate::mdy(str_replace_all(str_extract(X3, "[^\\s]+"), '[*]', ''))
        ) %>%
        # Don't include first date - weirdly has same vintage date as second date
        dplyr::filter(., releaseDate >= as.Date('2000-01-01'))
        
    paramsDf =
        tribble(
          ~ varname, ~ spfname, ~ method,
          'gdp', 'RGDP', 'growth',
          'pce', 'RCONSUM', 'growth',
          'ue', 'UNEMP', 'level',
          't03m', 'TBILL', 'level',
          't10y', 'TBOND', 'level',
          'houst', 'HOUSING', 'level',
          'inf', 'CORECPI', 'level'
          )
  
  
	df =
		lapply(c('level', 'growth'), function(m) {
		    file = file.path(DL_DIR, paste0('spf-', m, '.xlsx'))
	        httr::GET(
	            paste0(
	                'https://www.philadelphiafed.org/-/media/frbp/assets/surveys-and-data/survey-of-professional-forecasters/historical-data/median', m, '.xlsx?la=en'),
	                httr::write_disk(file, overwrite = TRUE)
	            )
	        
	        lapply(paramsDf %>% dplyr::filter(., method == m) %>% purrr::transpose(.), function(x) {
	            readxl::read_excel(file, na = '#N/A', sheet = x$spfname) %>%
	            dplyr::select(
	            .,
	            c('YEAR', 'QUARTER', {
	              if (m == 'level') paste0(x$spfname, 2:6) else paste0('d', str_to_lower(x$spfname), 2:6)
	              })
	            ) %>%
	            dplyr::mutate(., releaseDate = econforecasting::strdateToDate(paste0(YEAR, 'Q', QUARTER))) %>%
	            dplyr::select(., -YEAR, -QUARTER) %>%
	            tidyr::pivot_longer(., -releaseDate, names_to = 'fcPeriods') %>%
	            dplyr::mutate(., fcPeriods = as.numeric(str_sub(fcPeriods, -1)) - 2) %>%
	            dplyr::mutate(., obsDate = add_with_rollback(releaseDate, months(fcPeriods * 3))) %>%
	            na.omit(.) %>%
	            dplyr::inner_join(., vintageDf, by = 'releaseDate') %>%
	            dplyr::transmute(., fcname = 'spf', varname = x$varname, freq = 'q', form = 'd1', vdate = vintageDate, date = obsDate, value)
	            }) %>%
	            dplyr::bind_rows(.) %>%
	            return(.)
	        
	        }) %>%
	        dplyr::bind_rows(.)
        
	m$ext$sources$spf <<- df
})
```

## WSJ Economic Survey
WSJ Survey Updated to Quarterly - see https://www.wsj.com/amp/articles/economic-forecasting-survey-archive-11617814998
```{r}
local({
    
    orgsDf =
        tibble(
          fcname = c('wsj', 'fnm', 'wfc', 'gsu', 'spg', 'ucl', 'gsc'),
          fcfullname = c('WSJ Consensus', 'Fannie Mae', 'Wells Fargo & Co.', 'Georgia State University', 'S&P Global Ratings', 'UCLA Anderson Forecast', 'Goldman, Sachs & Co.')
        )
    
    filePaths =
        seq(as.Date('2021-01-01'), to = add_with_rollback(p$VINTAGE_DATE, months(0)), by = '3 months') %>%
        purrr::map(., function(x)
            list(
                date = x,
                file = paste0('wsjecon', str_sub(x, 6, 7), str_sub(x, 3, 4), '.xls')
                )
            )
    
    df =
        lapply(filePaths, function(x) {
            message(x$date)
            dest = file.path(DL_DIR, 'wsj.xls')

            httr::http_error('https://google.com')
            download.file(
                paste0('https://online.wsj.com/public/resources/documents/', x$file),
                destfile = dest,
                mode = 'wb',
                quiet = TRUE
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
                            vdate = as_date(x$date)
                            )
                    ) %>%
                dplyr::right_join(., orgsDf, by = 'fcfullname') %>%
                dplyr::select(., -fcfullname)
        }) %>%
        dplyr::bind_rows(.) %>%
    	na.omit(.)

            
    m$ext$sources$wsj <<- df   
})
```


## CBO Forecasts
```{r}
local({

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
      dplyr::filter(., date >= as.Date('2018-01-01'))

    
  tempPath = file.path(DL_DIR, 'cbo.xlsx')
  
  paramsDf =
    tribble(
      ~ varname, ~ cboCategory, ~ cboname, ~ cboUnits,
      'gdp', 'Output', 'Real GDP', 'Percentage change, annual rate',
      'pcepi', 'Prices', 'Price Index, Personal Consumption Expenditures (PCE)', 'Percentage change, annual rate',
      'inf', 'Prices', 'Consumer Price Index, All Urban Consumers (CPI-U)', 'Percentage change, annual rate',
      'oil', 'Prices', 'Price of Crude Oil, West Texas Intermediate (WTI)', 'Dollars per barrel',
      'ue', 'Labor', 'Unemployment Rate, Civilian, 16 Years or Older', 'Percent',
      'ffr', 'Interest Rates', 'Federal Funds Rate', 'Percent',
      'pce', 'Components of GDP (Real)', 'Personal Consumption Expenditures', 'Percentage change, annual rate',
      't10y', 'Interest Rates', '10-Year Treasury Note', 'Percent',
      't03m', 'Interest Rates', '3-Month Treasury Bill', 'Percent'
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
          skip = {if (as.Date(x$date, origin = lubridate::origin) == '2019-01-01') 5 else 6}
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
        tidyr::pivot_longer(-varname, names_to = 'obsDate') %>%
        dplyr::mutate(., obsDate = econforecasting::strdateToDate(obsDate)) %>%
        dplyr::filter(., obsDate >= as.Date(x$date, origin = lubridate::origin)) %>%
        dplyr::mutate(., vintageDate = as.Date(x$date, origin = lubridate::origin))
      }) %>%
    dplyr::bind_rows(.) %>%
    dplyr::transmute(., fcname = 'cbo', varname, form = 'd1', freq = 'q', date = obsDate, vdate = vintageDate, value)
  
  # Count number of forecasts per group
  # df %>% dplyr::group_by(vintageDate, varname) %>% dplyr::summarize(., n = n()) %>% View(.)
  
    
  m$ext$sources$cbo <<- df   
})
```


## EINF Model - Cleveland Fed
```{r}
local({
	
  file = file.path(DL_DIR, paste0('inf.xls'))

  download.file('https://www.clevelandfed.org/en/our-research/indicators-and-data/~/media/content/our%20research/indicators%20and%20data/inflation%20expectations/ie%20latest/ie%20xls.xls', file, mode = 'wb')

  df =
	readxl::read_excel(file, sheet = 'Expected Inflation') %>%
  	dplyr::rename(., vintageDate = 'Model Output Date') %>%
  	tidyr::pivot_longer(., -vintageDate, names_to = 'ttm', values_to = 'yield') %>%
  	dplyr::mutate(
  		.,
  		vintageDate = as.Date(vintageDate), ttm = as.numeric(str_replace(str_sub(ttm, 1, 2), ' ', '')) * 12
  		) %>%
  	dplyr::filter(., vintageDate == max(vintageDate)) %>%
  	dplyr::right_join(., tibble(ttm = 1:360), by = 'ttm') %>%
  	dplyr::arrange(., ttm) %>%
  	dplyr::mutate(
  		.,
  		yield = zoo::na.spline(yield),
  		vintageDate = unique(na.omit(vintageDate)),
  		curDate = floor_date(p$VINTAGE_DATE, 'months'),
  		cumReturn = (1 + yield)^(ttm/12),
  		yttmAheadCumReturn = dplyr::lead(cumReturn, 1)/cumReturn,
  		yttmAheadAnnualizedYield = (yttmAheadCumReturn^(12/1) - 1) * 100,
  		obsDate = add_with_rollback(curDate, months(ttm - 1))
  		) %>%
  	dplyr::transmute(
  		.,
		fcname = 'cle',
		varname = 'inf',
        form = 'd1',
		freq = 'm',
		date = obsDate,
  		vdate = vintageDate,
  		value = yttmAheadAnnualizedYield,
  		) %>%
  	na.omit(.)

    m$ext$sources$cle <<- df
})
```

## CME Model
```{r}
local({
  
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
				dplyr::transmute(., vintageDate = Date, settle = Settle, j = j) %>%
				dplyr::filter(., vintageDate >= as.Date('2010-01-01'))
			}) %>%
		dplyr::bind_rows(.) %>%
		dplyr::transmute(
			.,
			vintageDate,
		# Consider the forecasted period the vintageDate + j
			obsDate =
				econforecasting::strdateToDate(paste0(year(vintageDate), 'M', month(vintageDate))) %>%
				lubridate::add_with_rollback(., months(j - 1), roll_to_first = TRUE),
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
						obsDate = lubridate::ymd(x$expirationDate),
						value = 100 - as.numeric(x$priorSettle),
						varname = var$varname
						)
					}) %>%
				return(.)
			}) %>% 
		# Now average out so that there's only one value for each (varname, obsDate) combo
		dplyr::group_by(varname, obsDate) %>%
		dplyr::summarize(., value = mean(value), .groups = 'drop') %>%
		dplyr::arrange(., obsDate) %>%
		# Get rid of forecasts for old observations
		dplyr::filter(., obsDate >= lubridate::floor_date(Sys.Date(), 'month')) %>%
		# Assume vintagedate is the same date as the last Quandl obs
		dplyr::mutate(., vintageDate = lastTradeDate, fcname = 'cme') %>%
		dplyr::filter(., value != 100)
	
	
	message('Completed CME data scrape...')
	
	# Now combine, replacing df2 with df1 if necessary
	combinedDf =
		dplyr::full_join(df, df2, by = c('fcname', 'vintageDate', 'obsDate', 'varname')) %>%
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
		dplyr::group_by(vintageDate, varname) %>%
		dplyr::group_split(.) %>%
		lapply(., function(x) {
			x %>%
				# Join on missing obs dates
				dplyr::right_join(
					.,
					tibble(obsDate = seq(from = .$obsDate[[1]], to = tail(.$obsDate, 1), by = '1 month')) %>%
						dplyr::mutate(n = 1:nrow(.)),
					by = 'obsDate'
					) %>%
				dplyr::arrange(obsDate) %>%
				dplyr::transmute(
					.,
					fcname = head(fcname, 1),
					varname = head(varname, 1),
					form = 'd1',
					freq = 'm',
					date = obsDate,
					vdate = head(vintageDate, 1),
					value = zoo::na.spline(value)
					)		
			}) %>%
		dplyr::bind_rows(.)

	m$ext$sources$cme <<- finalDf
})
```

## DNS - TDNS1, TDNS2, TDNS3, Treasury Yields, and Spreads
DIEBOLD LI FUNCTION SHOULD BE ffr + f1 + f2 () + f3()
Calculated TDNS1: TYield_10y 
Calculated TDNS2: -1 * (t10y - t03m)
Calculated TDNS3: .3 * (2*t02y - t03m - t10y)
Keep these treasury yield forecasts as the external forecasts ->
note that later these will be "regenerated" in the baseline calculation, may be off a bit due to calculation from TDNS, compare to
```{r}
local({
    
    dnsCoefs = m$dnsCoefs
    dnsLambda = m$dnsLambda
    dnsYieldCurveNamesMap = m$dnsYieldCurveNamesMap
    
    # Monthly forecast up to 10 years
    # Get cumulative return starting from curDate
    fittedCurve =
        tibble(ttm = seq(1: 480)) %>%
        dplyr::mutate(., curDate = floor_date(p$VINTAGE_DATE, 'months')) %>%
        dplyr::mutate(
          .,
          annualizedYield = 
            dnsCoefs$b1 +
            dnsCoefs$b2 * (1-exp(-1 * dnsLambda * ttm))/(dnsLambda * ttm) +
            dnsCoefs$b3 * ((1-exp(-1 * dnsLambda * ttm))/(dnsLambda * ttm) - exp(-1 * dnsLambda * ttm)),
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
        dnsYieldCurveNamesMap$ttm %>%
        lapply(., function(yttm)
            fittedCurve %>%
                dplyr::mutate(
                    .,
                    yttmAheadCumReturn = dplyr::lead(cumReturn, yttm)/cumReturn,
                    yttmAheadAnnualizedYield = (yttmAheadCumReturn^(12/yttm) - 1) * 100
                    ) %>%
                dplyr::filter(., ttm <= 120) %>%
                dplyr::mutate(., yttm = yttm) %>%
                dplyr::inner_join(., dnsYieldCurveNamesMap, c('yttm' = 'ttm'))
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
            vdate = p$VINTAGE_DATE,
            value = yttmAheadAnnualizedYield
            )
    
    # Add ffr to forecasts
    df1 =
        df0 %>%
        dplyr::select(., varname, date, value) %>%
        dplyr::inner_join(
            .,
            m$ext$sources$cme %>%
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
            vdate = p$VINTAGE_DATE,
            value
            )

    # Calculate TDNS yield forecasts
    # Forecast vintage date should be bound to historical data vintage date since reliant purely on historical data
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
        dplyr::transmute(., fcname = 'dns', varname, date, form = 'd1', freq = 'm', vdate = p$VINTAGE_DATE, value)
    
    m$ext$sources$dns <<- dplyr::bind_rows(df1, df2)
})
```

## Get Combined Data
```{r}
local({
	
	predFlat =
		m$ext$sources %>%
		dplyr::bind_rows(.) %>%
		dplyr::transmute(., fcname, vdate, freq, form, varname, date, value)

	if (nrow(na.omit(predFlat)) != nrow(predFlat)) stop('Missing obs')
	
	m$ext$predFlat <<- predFlat
})
```
