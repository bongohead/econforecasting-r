## ----purl = TRUE-------------------------------------------------
DIR = 'D:/Onedrive/__Projects/econforecasting'
PACKAGE_DIR = 'D:/Onedrive/__Projects/econforecasting/r-package' # Path to package with helper functions
DOC_DIR = 'D:/Onedrive/__Projects/econforecasting/documentation-templates' # Path to documentation .RNW file; if NULL no docs generated
INPUT_DIR = 'D:/Onedrive/__Projects/econforecasting/model-inputs' # Path to directory with inputs.r, constants.r (SQL DB info, SFTP info, etc.)
OUTPUT_DIR = 'D:/Onedrive/__Projects/econforecasting/model-outputs' # Path to location to output RDS and documentation
RESET_ALL = FALSE
VINTAGE_DATE = as.Date('2021-05-21')#Sys.Date() # Date to pull data "as-of", for general use set this as Sys.Date(); otherwise set as an older date for backtesting


## ----------------------------------------------------------------
# General purpose
library(tidyverse) # General
library(data.table) # General
library(devtools) # General
library(lubridate) # Dates
library(glue) # String Interpolation
# Data parse/import
library(jsonlite) # JSON Parser
library(rvest) # HTML Parser
library(httr) # CURL Interface
# SQL/Apache Spark
library(DBI) # SQL Interface
# library(RPostgres) # PostgreSQL
# library(rsparklyr) # Spark
# My package
devtools::load_all(path = PACKAGE_DIR)
devtools::document(PACKAGE_DIR)
# library(econforecasting)

# Set working directory
setwd(DIR)

# Read constants
source(file.path(INPUT_DIR, 'constants.r'))

# Create top level variables
ef = list(
  h = list(),
  f = list()
)


## ----------------------------------------------------------------
local({
	
	fredRes =
		readxl::read_excel(file.path(INPUT_DIR, 'inputs.xlsx'), sheet = 'params') %>%
		purrr::transpose(., .names = .$varname)  %>%
		purrr::keep(., ~ .$source == 'fred') %>%
		lapply(., function(x) {
			
			message('Getting data ... ', x$varname)

			# Get series data
			dataDf =
			  econforecasting::getDataFred(x$sckey, CONST$FRED_API_KEY, .freq = 'q', .returnVintages = TRUE, .vintageDate = VINTAGE_DATE) %>%
			  	dplyr::filter(., vintageDate <= VINTAGE_DATE) %>%
			  	dplyr::filter(., vintageDate == max(vintageDate)) %>%
			  	dplyr::select(., -vintageDate) %>%
			    dplyr::mutate(., varname = x$varname, freq = 'q') %>%
			    {if (x$freq %in% c('d', 'm'))
			      dplyr::bind_rows(
			        .,
			        econforecasting::getDataFred(x$sckey, CONST$FRED_API_KEY, .freq = 'm', .returnVintages = TRUE, .vintageDate = VINTAGE_DATE) %>%
						dplyr::filter(., vintageDate <= VINTAGE_DATE) %>%
						dplyr::filter(., vintageDate == max(vintageDate)) %>%
						dplyr::select(., -vintageDate) %>%
				        dplyr::mutate(., varname = x$varname, freq = 'm')
			        )
			      else .
			    } %>%
			    {if (x$freq %in% c('d'))
			      dplyr::bind_rows(
			        .,
			        econforecasting::getDataFred(x$sckey, CONST$FRED_API_KEY, .freq = 'd', .returnVintages = TRUE, .vintageDate = VINTAGE_DATE) %>%
					  	dplyr::filter(., vintageDate <= VINTAGE_DATE) %>%
					  	dplyr::filter(., vintageDate == max(vintageDate)) %>%
					  	dplyr::select(., -vintageDate) %>%
				        dplyr::mutate(., varname = x$varname, freq = 'd')
			        )
			      else .
			    }
			
			# Get series release
			releaseDf =
				httr::RETRY(
					'GET', 
					glue('https://api.stlouisfed.org/fred/series/release?series_id={x$sckey}&api_key={CONST$FRED_API_KEY}&file_type=json'),
					times = 10
				) %>%
		        httr::content(., as = 'parsed') %>%
				.$releases %>%
				.[[1]] %>%
				as_tibble(.) %>%
				dplyr::mutate(., varname = x$varname)

			list(dataDf = dataDf, releaseDf = releaseDf)
		}) %>%
		lapply(., function(x) list(dataDf = x$dataDf, releaseDf = x$releaseDf %>% dplyr::rename(., relname = name)))
	
	
	
	paramsDf =
		readxl::read_excel(file.path(INPUT_DIR, 'inputs.xlsx')) %>%
		dplyr::left_join(
			.,
			lapply(fredRes, function(x) x$releaseDf) %>%
			    dplyr::bind_rows(.) %>%
				dplyr::transmute(., varname, releaseid = id),
			by = 'varname'
		)
	
	params = paramsDf %>% purrr::transpose(., .names = .$varname)

	
	
	dataDf =
		lapply(fredRes, function(x) x$dataDf) %>%
		dplyr::bind_rows(.) %>%
		dplyr::rename(., date = obsDate) %>%
		dplyr::filter(., date >= as.Date('2010-01-01'))
	
	releaseDf =
		lapply(fredRes, function(x) x$releaseDf) %>%
		dplyr::bind_rows(.) %>%
		# Now create a column of included varnames
		dplyr::inner_join(
			.,
			# Only included releases relevant to dfminputs
			paramsDf %>% dplyr::filter(., dfminput == TRUE) %>% .[, c('varname', 'fullname', 'releaseid')],
			by = c('varname')
			) %>%
		dplyr::group_by(., id, relname, link) %>%
		dplyr::summarize(., count = n(), seriesnames = jsonlite::toJSON(fullname), .groups = 'drop') %>%
		# Now create a column of included releaseDates
		dplyr::inner_join(
			.,
			purrr::map_dfr(.$id, function(id)
				httr::RETRY(
					'GET', 
					glue(
						'https://api.stlouisfed.org/fred/release/dates?',
						'release_id={id}&realtime_start=2020-01-01',
						'&include_release_dates_with_no_data=true&api_key={CONST$FRED_API_KEY}&file_type=json'
						 ),
					times = 10
					) %>%
			        httr::content(., as = 'parsed') %>%
					.$release_dates %>%
					sapply(., function(y) y$date) %>%
					tibble(id = id, reldates = .)
				),
			by = 'id'
		) %>%
		dplyr::group_by(., id, relname, link, count, seriesnames) %>%
		dplyr::summarize(., reldates = jsonlite::toJSON(reldates), .groups = 'drop')


	# Run below in console to get list of GDP subcomponents
	# httr::GET('https://fred.stlouisfed.org/release/tables?rid=53&eid=14961#snid=14966') %>%
	# 	httr::content(.) %>%
	# 	rvest::html_nodes('#release-elements-tree tbody tr') %>%
	# 	map_dfr(., function(x)
	# 		tibble(
	# 			a =
	# 				x %>% rvest::html_nodes('td') %>% .[3] %>%
	# 				html_text(.) %>% str_replace_all(., '\n', '') %>% trimws(.),
	# 			fred =
	# 				x %>% rvest::html_nodes('td') %>% .[3] %>%
	# 				rvest::html_nodes('span a') %>% rvest::html_attr('href') %>% str_replace(., '/series/', '')
	# 			)
	# 		)

	
	ef$paramsDf <<- paramsDf
	ef$params <<- params

	ef$h$source$fred <<- dataDf
	ef$releaseDf <<- releaseDf
})


## ----------------------------------------------------------------
local({
	
	df =
		ef$params %>%
    	purrr::keep(., ~ .$source == 'yahoo') %>%
		lapply(., function(x) {
		
			url =
				paste0(
					'https://query1.finance.yahoo.com/v7/finance/download/', x$sckey,
					'?period1=', '946598400', # 12/30/1999
					'&period2=', as.numeric(as.POSIXct(Sys.Date() + lubridate::days(1))),
					'&interval=1d',
					'&events=history&includeAdjustedClose=true'
				)
			
			df0 =
				data.table::fread(url) %>%
				.[, c('Date', 'Adj Close')]	%>%
				setnames(., new = c('date', 'value')) %>%
				as_tibble(.) %>%
				dplyr::filter(., date <= VINTAGE_DATE)
			
			qDf = 
				df0 %>%
				dplyr::mutate(
					.,
					date = econforecasting::strdateToDate(paste0(lubridate::year(date), 'Q', lubridate::quarter(date)))
					) %>%
				dplyr::group_by(., date) %>%
				dplyr::summarize(., value = mean(value), .groups = 'drop') %>%
				dplyr::mutate(., freq = 'q')
			
			mDf =
				df0 %>%
				dplyr::mutate(
					.,
					date = econforecasting::strdateToDate(paste0(lubridate::year(date), 'M', lubridate::month(date)))
					) %>%
				dplyr::group_by(., date) %>%
				dplyr::summarize(., value = mean(value), .groups = 'drop') %>%
				dplyr::mutate(., freq = 'm')
		
			df =
				dplyr::bind_rows(mDf, qDf) %>%
				dplyr::mutate(., varname = x$varname)
		}) %>%
		dplyr::bind_rows(.)
	
	ef$h$source$yahoo <<- df
})


## ----------------------------------------------------------------
local({
	ef$h$sourceDf <<- dplyr::bind_rows(ef$h$source)
})


## ----------------------------------------------------------------
local({
	
	seasDf =
		ef$h$sourceDf %>%
		dplyr::filter(., varname == 'hpils') %>%
		dplyr::mutate(
			.,
			seas = 
			{ts(.$value, start = c(year(.$date[1]), month(.$date[1])), freq = 12)} %>%
			seasonal::seas(.) %>%
			predict(.)
			) %>%
		dplyr::select(., -value)
	
	df =
		dplyr::left_join(ef$h$sourceDf, seasDf, by = c('date', 'varname', 'freq')) %>%
		dplyr::mutate(., value = ifelse(is.na(seas), value, seas)) %>%
		dplyr::select(., -seas)

	ef$h$seasDf <<- df
})


## ----------------------------------------------------------------
local({

	flat = list()

	flat =
		lapply(list('st', 'd1', 'd2') %>% setNames(., .), function(.form)
			ef$h$seasDf %>%
				dplyr::left_join(., ef$paramsDf[, c('varname', .form)], by = c('varname')) %>%
				dplyr::group_by(varname, freq) %>%
				dplyr::group_split(.) %>%
				lapply(., function(x) {
					# message(x$varname[[1]])
					if (head(x, 1)[[.form]] == 'none') return(NA)
					x %>%
						dplyr::arrange(., date) %>%
						dplyr::mutate(
							.,
							value = {
								if (head(., 1)[[.form]] == 'base') value
								else if (head(., 1)[[.form]] == 'dlog') dlog(value)
								else if (head(., 1)[[.form]] == 'diff1') diff1(value)
								else if (head(., 1)[[.form]] == 'diff2') diff2(value)
								else if (head(., 1)[[.form]] == 'ma2') ma2(value)
								else if (head(., 1)[[.form]] == 'lma2') lma2(value)
								else if (head(., 1)[[.form]] == 'apchg') apchg(value)
								else stop ('Error')
								}
							)
					}) %>%
				purrr::keep(., ~ !all(is.na(.))) %>%
				dplyr::bind_rows(.) %>%
				dplyr::select(., -all_of(c(.form)))
			)
	
	flat$ut = ef$h$seasDf

	
	flatDf = purrr::imap_dfr(flat, function(x, form) x %>% mutate(., form = form)) %>% na.omit(.)

	ef$h$flatDf <<- flatDf
})


## ----------------------------------------------------------------
local({
	
	wide =
		ef$h$flatDf %>%
		as.data.table(.) %>%
		split(., by = 'freq') %>%
		lapply(., function(x)
			split(x, by = 'form') %>%
				lapply(., function(y)
					as_tibble(y) %>%
						dplyr::select(., -freq, -form) %>%
						tidyr::pivot_wider(., names_from = varname) %>%
						dplyr::arrange(., date)
					)
			)

	ef$h$m <<- wide$m
	ef$h$q <<- wide$q
})


## ----------------------------------------------------------------
local({
	
	quartersForward = 1
	
	pcaVarnames = ef$paramsDf %>% dplyr::filter(., dfminput == TRUE) %>% .$varname
	
	pcaVariablesDf =
		ef$h$m$st %>%
		dplyr::select(., date, all_of(pcaVarnames)) %>%
		dplyr::filter(., date >= as.Date('2010-01-01'))
	
	bigTDates = pcaVariablesDf %>% dplyr::filter(., !if_any(everything(), is.na)) %>% .$date
	bigTauDates = pcaVariablesDf %>% dplyr::filter(., if_any(everything(), is.na)) %>% .$date
	bigTStarDates =
		bigTauDates %>%
		tail(., 1) %>%
		seq(
			from = .,
			to =
				# Get this quarter
				econforecasting::strdateToDate(paste0(lubridate::year(.), 'Q', lubridate::quarter(.))) %>%
				# Get next quarter minus a month
				lubridate::add_with_rollback(., months(3 * (1 + quartersForward) - 1)),
			by = '1 month'
			) %>%
		.[2:length(.)]
	
	
	bigTDate = tail(bigTDates, 1)
	bigTauDate = tail(bigTauDates, 1)
	bigTStarDate = tail(bigTauDates, 1)
	
	bigT = length(bigTDates)
	bigTau = bigT + length(bigTauDates)
	bigTStar = bigTau + length(bigTStarDates)
	
	timeDf =
		tibble(
			date = as.Date(c(pcaVariablesDf$date[[1]], bigTDate, bigTauDate, bigTStarDate)),
			time = c('1', 'T', 'Tau', 'T*')
		)
	
	

	ef$nc$pcaVariablesDf <<- pcaVariablesDf
	ef$nc$pcaVarnames <<- pcaVarnames
	ef$nc$quartersForward <<- quartersForward
	ef$nc$bigTDates <<- bigTDates
	ef$nc$bigTauDates <<- bigTauDates
	ef$nc$bigTStarDates <<- bigTStarDates
	ef$nc$bigTDate <<- bigTDate
	ef$nc$bigTauDate <<- bigTauDate
	ef$nc$bigTStarDate <<- bigTStarDate
	ef$nc$bigT <<- bigT
	ef$nc$bigTau <<- bigTau
	ef$nc$bigTStar <<- bigTStar
	ef$nc$timeDf <<- timeDf
})


## ----------------------------------------------------------------
local({
	
	xDf = ef$nc$pcaVariablesDf %>% dplyr::filter(., date %in% ef$nc$bigTDates)
	
	xMat =
		xDf %>%
		dplyr::select(., -date) %>%
		as.matrix(.) %>%
		scale(.)
	
	lambdaHat = eigen(t(xMat) %*% xMat) %>% .$vectors
	fHat = (xMat) %*% lambdaHat
	bigN = ncol(xMat)
	bigT = nrow(xMat)
	bigCSquared = min(bigN, bigT)
	
	# Total variance of data
	totalVar = xMat %>% cov(.) %>% diag(.) %>% sum(.)
	
	# Calculate ICs from Bai and Ng (2002)
	# Total SSE should be approx 0 due to normalization above;
	# sapply(1:ncol(xMat), function(i)
	# 	sapply(1:nrow(xMat), function(t)
	# 		(xMat[i, 1] - matrix(lambdaHat[i, ], nrow = 1) %*% matrix(fHat[t, ], ncol = 1))^2
	# 		) %>% sum(.)
	# 	) %>%
	# 	sum(.) %>%
	# 	{./(ncol(xMat) %*% nrow(xMat))}
	(xMat - (fHat %*% t(lambdaHat)))^1
	
	# Now test by R
	mseByR =
		sapply(1:bigN, function(r)
			sum((xMat - (fHat[, 1:r, drop = FALSE] %*% t(lambdaHat)[1:r, , drop = FALSE]))^2)/(bigT * bigN)
			)
	
	
	# Explained variance of data
	screeDf =
		fHat %>% cov(.) %>% diag(.) %>%
		{lapply(1:length(.), function(i)
			tibble(
				factors = i,
				var_explained_by_factor = .[i],
				pct_of_total = .[i]/totalVar,
				cum_pct_of_total = sum(.[1:i])/totalVar
				)
			)} %>%
		dplyr::bind_rows(.) %>%
		dplyr::mutate(., mse = mseByR) %>%
		dplyr::mutate(
			.,
			ic1 = (mse) + factors * (bigN + bigT)/(bigN * bigT) * log((bigN * bigT)/(bigN + bigT)),
			ic2 = (mse) + factors * (bigN + bigT)/(bigN * bigT) * log(bigCSquared),
			ic3 = (mse) + factors * (log(bigCSquared)/bigCSquared)
		)
	
	screePlot =
		screeDf %>%
    	ggplot(.) +
    	geom_col(aes(x = factors, y = cum_pct_of_total, fill = factors)) +
		# geom_col(aes(x = factors, y = pct_of_total)) +
    	labs(title = 'Percent of Variance Explained', x = 'Factors (R)', y = 'Cumulative % of Total Variance Explained', fill = NULL) + 
    	ggthemes::theme_fivethirtyeight()

	
	bigR =
		screeDf %>%
		dplyr::filter(., ic1 == min(ic1)) %>%
		.$factors + 1
		# ((
		# 	{screeDf %>% dplyr::filter(cum_pct_of_total >= .80) %>% head(., 1) %>%.$factors} +
		#   	{screeDf %>% dplyr::filter(., ic1 == min(ic1)) %>% .$factors}
		# 	)/2) %>%
		# round(., digits = 0)
	
	zDf =
		xDf[, 'date'] %>%
		dplyr::bind_cols(
			.,
			fHat[, 1:bigR] %>% as.data.frame(.) %>% setNames(., paste0('f', 1:bigR))
			)
	

	zPlots =
		purrr::imap(colnames(zDf) %>% .[. != 'date'], function(x, i)
			dplyr::select(zDf, all_of(c('date', x))) %>%
				setNames(., c('date', 'value')) %>%
				ggplot() +
				geom_line(
					aes(x = date, y = value),
					color = hcl(h = seq(15, 375, length = bigR + 1), l = 65, c = 100)[i]
					) +
				labs(x = NULL, y = NULL, title = paste0('Estimated PCA Factor ', str_sub(x, -1), ' Plot')) +
				ggthemes::theme_fivethirtyeight() +
				scale_x_date(date_breaks = '1 year', date_labels = '%Y')
			)
	
	factorWeightsDf =
		lambdaHat %>%
		as.data.frame(.) %>% 
		as_tibble(.) %>%
		setNames(., paste0('f', str_pad(1:ncol(.), pad = '0', 1))) %>%
		dplyr::bind_cols(weight = colnames(xMat), .) %>%
		tidyr::pivot_longer(., -weight, names_to = 'varname') %>%
		dplyr::group_by(varname) %>%
		dplyr::arrange(., varname, desc(abs(value))) %>%
		dplyr::mutate(., order = 1:n(), valFormat = paste0(weight, ' (', round(value, 2), ')')) %>%
		dplyr::ungroup(.) %>%
		dplyr::select(., -value, -weight) %>%
		tidyr::pivot_wider(., names_from = varname, values_from = valFormat) %>%
		dplyr::arrange(., order) %>%
		dplyr::select(., -order) %>%
		dplyr::select(., paste0('f', 1:bigR))
	
	
	ef$nc$factorWeightsDf <<- factorWeightsDf
	ef$nc$screeDf <<- screeDf
	ef$nc$screePlot <<- screePlot
	ef$nc$bigR <<- bigR
	ef$nc$pcaInputDf <<- xDf
	ef$nc$zDf <<- zDf
	ef$nc$zPlots <<- zPlots
})


## ----------------------------------------------------------------
local({
	
	inputDf =
		ef$nc$zDf %>%
    	econforecasting::addLags(., 1, TRUE) %>%
		na.omit(.) #%>%
		# dplyr::filter(., date <= '2020-02-01' | date >= '2020-09-01')
	
	yMat = inputDf %>% dplyr::select(., -contains('.l'), -date) %>% as.matrix(.)
	xDf = inputDf %>% dplyr::select(., contains('.l')) %>% dplyr::bind_cols(constant = 1, .)
	
	# solve(t(xMat) %*% xMat) %*% (t(xMat) %*% yMat)
	coefDf =
		lm(yMat ~ . - 1, data = xDf) %>%
		coef(.) %>%
		as.data.frame(.) %>%
		rownames_to_column(., 'coefname') %>%
		as_tibble(.) 
	
	gofDf =
		lm(yMat ~ . - 1, xDf) %>%
		resid(.) %>%
		as.data.frame(.) %>%
		as_tibble(.) %>%
		tidyr::pivot_longer(., everything(), names_to = 'varname') %>%
		dplyr::group_by(., varname) %>%
		dplyr::summarize(., MAE = mean(abs(value)), MSE = mean(value^2))

	residPlot =
		lm(yMat ~ . - 1, xDf) %>%
	    resid(.) %>%
	    as.data.frame(.) %>%
	    as_tibble(.) %>%
	    dplyr::bind_cols(date = inputDf$date, .) %>%
		tidyr::pivot_longer(., -date) %>%
		ggplot(.) + 	
		geom_line(aes(x = date, y = value, group = name, color = name), size = 1) +
		labs(title = 'Residuals plot for PCA factors', x = NULL, y = NULL, color = NULL) +
		ggthemes::theme_fivethirtyeight()
	
	fittedPlots =
		lm(yMat ~ . - 1, data = xDf) %>%
    	fitted(.) %>%
		as_tibble(.) %>%
		dplyr::bind_cols(date = inputDf$date, ., type = 'Fitted Values') %>%
		dplyr::bind_rows(., ef$nc$zDf %>% dplyr::mutate(., type = 'Data')) %>%
		tidyr::pivot_longer(., -c('type', 'date')) %>% 
		as.data.table(.) %>%
		split(., by = 'name') %>%
		purrr::imap(., function(x, i)
			ggplot(x) +
				geom_line(
					aes(x = date, y = value, group = type, linetype = type, color = type),
					size = 1, alpha = 1.0
					) +
				labs(
					x = NULL, y = NULL, color = NULL, linetype = NULL,
					title = paste0('Fitted values vs actual for factor ', i)
					) +
				scale_x_date(date_breaks = '1 year', date_labels = '%Y') +
				ggthemes::theme_fivethirtyeight()
			)
	
	bMat = coefDf %>% dplyr::filter(., coefname != 'constant') %>% dplyr::select(., -coefname) %>% t(.)
	cMat = coefDf %>% dplyr::filter(., coefname == 'constant') %>% dplyr::select(., -coefname) %>% t(.)
	qMat =
		lm(yMat ~ . - 1, data = xDf) %>%
	    residuals(.) %>% 
		as_tibble(.) %>%
		purrr::transpose(.) %>%
		lapply(., function(x) as.numeric(x)^2 %>% diag(.)) %>%
		{purrr::reduce(., function(x, y) x + y)/length(.)}
	
	
	ef$nc$varFittedPlots <<- fittedPlots
	ef$nc$varResidPlot <<- residPlot
	ef$nc$varGofDf <<- gofDf
	ef$nc$varCoefDf <<- coefDf
	ef$nc$qMat <<- qMat 
	ef$nc$bMat <<- bMat
	ef$nc$cMat <<- cMat
})


## ----------------------------------------------------------------
local({
	
	yMat = ef$nc$pcaInputDf %>% dplyr::select(., -date) %>% as.matrix(.)
	xDf = ef$nc$zDf %>% dplyr::select(., -date) %>% dplyr::bind_cols(constant = 1, .)
	
	coefDf =
		lm(yMat ~ . - 1, xDf) %>%
		coef(.) %>%	
		as.data.frame(.) %>%
    	rownames_to_column(., 'coefname') %>%
		as_tibble(.)
	
	
	fittedPlots =
		lm(yMat ~ . - 1, xDf) %>%
	    fitted(.) %>%
	    as_tibble(.) %>%
	    dplyr::bind_cols(date = ef$nc$zDf$date, ., type = 'Fitted Values') %>%
	    dplyr::bind_rows(., ef$nc$pcaInputDf %>% dplyr::mutate(., type = 'Data')) %>%
		tidyr::pivot_longer(., -c('type', 'date')) %>%
		as.data.table(.) %>%
		split(., by = 'name') %>%
		purrr::imap(., function(x, i)
        	ggplot(x) +
	            geom_line(
	                aes(x = date, y = value, group = type, linetype = type, color = type),
	                size = 1, alpha = 1.0
	            ) +
	            labs(
	                x = NULL, y = NULL, color = NULL, linetype = NULL,
	                title = paste0('Fitted values vs actual for factor ', i)
	            ) +
	            scale_x_date(date_breaks = '1 year', date_labels = '%Y') +
	            ggthemes::theme_fivethirtyeight()
	    )
		
	
	gofDf =
		lm(yMat ~ . - 1, xDf) %>%
		resid(.) %>%
		as.data.frame(.) %>%
		as_tibble(.) %>%
		tidyr::pivot_longer(., everything(), names_to = 'varname') %>%
		dplyr::group_by(., varname) %>%
		dplyr::summarize(., MAE = mean(abs(value)), MSE = mean(value^2))
	
	
	aMat = coefDf %>% dplyr::filter(., coefname != 'constant') %>% dplyr::select(., -coefname) %>% t(.)
	dMat = coefDf %>% dplyr::filter(., coefname == 'constant') %>% dplyr::select(., -coefname) %>% t(.)

	rMat0 =
		lm(yMat ~ . - 1, data = xDf) %>%
	    residuals(.) %>% 
		as_tibble(.) %>%
		purrr::transpose(.) %>%
		lapply(., function(x) as.numeric(x)^2 %>% diag(.)) %>%
		{purrr::reduce(., function(x, y) x + y)/length(.)}
	
	rMatDiag = tibble(varname = ef$nc$pcaVarnames, variance = diag(rMat0))

	
	rMats =
		lapply(ef$nc$bigTauDates, function(d)
			sapply(ef$nc$pcaVarnames, function(v)
				ef$h$m$st %>%
					dplyr::filter(., date == (d)) %>%
					.[[v]] %>% 
					{if (is.na(.)) 1e20 else dplyr::filter(rMatDiag, varname == v)$variance}
				) %>%
				diag(.)
			) %>%
		c(lapply(1:length(ef$nc$bigTDates), function(x) rMat0), .)

	
	ef$nc$dfmGofDf <<- gofDf
	ef$nc$dfmCoefDf <<- coefDf
	ef$nc$dfmFittedPlots <<- fittedPlots
	ef$nc$rMats <<- rMats
	ef$nc$aMat <<- aMat
	ef$nc$dMat <<- dMat
})


## ----------------------------------------------------------------
local({
	
	bMat = ef$nc$bMat
	cMat = ef$nc$cMat
	aMat = ef$nc$aMat
	dMat = ef$nc$dMat
	rMats = ef$nc$rMats
	qMat = ef$nc$qMat
	yMats =
		dplyr::bind_rows(
			ef$nc$pcaInputDf,
			ef$h$m$st %>%
				dplyr::filter(., date %in% ef$nc$bigTauDates) %>%
				dplyr::select(., date, ef$nc$pcaVarnames)
			) %>%
		dplyr::mutate(., across(-date, function(x) ifelse(is.na(x), 0, x))) %>%
		dplyr::select(., -date) %>%
		purrr::transpose(.) %>%
		lapply(., function(x) matrix(unlist(x), ncol = 1))

	z0Cond0 = matrix(rep(0, ef$nc$bigR), ncol = 1)
	sigmaZ0Cond0 = matrix(rep(0, ef$nc$bigR^2), ncol = ef$nc$bigR)
		
	zTCondTMinusOne = list()
	zTCondT = list()
	
	sigmaZTCondTMinusOne = list()
	sigmaZTCondT = list()
	
	yTCondTMinusOne = list()
	sigmaYTCondTMinusOne = list()
	
	pT = list()

	for (t in 1:length(c(ef$nc$bigTDates, ef$nc$bigTauDates))) {
		# message(t)
		# Prediction Step
		zTCondTMinusOne[[t]] = bMat %*% {if (t == 1) z0Cond0 else zTCondT[[t-1]]} + cMat
		sigmaZTCondTMinusOne[[t]] = bMat %*% {if (t == 1) sigmaZ0Cond0 else sigmaZTCondT[[t-1]]} + qMat
		yTCondTMinusOne[[t]] = aMat %*% zTCondTMinusOne[[t]] + dMat
		sigmaYTCondTMinusOne[[t]] = aMat %*% sigmaZTCondTMinusOne[[t]] %*% t(aMat) + rMats[[t]]
		
		# Correction Step
		pT[[t]] = sigmaZTCondTMinusOne[[t]] %*% t(aMat) %*%
			{
				if (t %in% 1:length(ef$nc$bigTDates)) solve(sigmaYTCondTMinusOne[[t]])
				else chol2inv(chol(sigmaYTCondTMinusOne[[t]]))
				}
		zTCondT[[t]] = zTCondTMinusOne[[t]] + pT[[t]] %*% (yMats[[t]] - yTCondTMinusOne[[t]])
		sigmaZTCondT[[t]] = sigmaZTCondTMinusOne[[t]] - (pT[[t]] %*% sigmaYTCondTMinusOne[[t]] %*% t(pT[[t]]))
	}
	
	
	kFitted =
		zTCondT %>%
		purrr::map_dfr(., function(x)
			as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
		dplyr::bind_cols(date = c(ef$nc$bigTDates, ef$nc$bigTauDates), .) 
	
	
	## Smoothing step
	zTCondBigTSmooth = list()
	sigmaZTCondBigTSmooth = list()
	sT = list()
	
	for (t in (length(zTCondT) - 1): 1) {
		# message(t)
		sT[[t]] = sigmaZTCondT[[t]] %*% t(bMat) %*% solve(sigmaZTCondTMinusOne[[t + 1]])
		zTCondBigTSmooth[[t]] = zTCondT[[t]] + sT[[t]] %*% 
			({if (t == length(zTCondT) - 1) zTCondT[[t + 1]] else zTCondBigTSmooth[[t + 1]]} - zTCondTMinusOne[[t + 1]])
		sigmaZTCondBigTSmooth[[t]] = sigmaZTCondT[[t]] - sT[[t]] %*%
			(sigmaZTCondTMinusOne[[t + 1]] -
			 	{if (t == length(zTCondT) - 1) sigmaZTCondT[[t + 1]] else sigmaZTCondBigTSmooth[[t + 1]]}
			 ) %*% t(sT[[t]])
	}
	
	kSmooth =
		zTCondBigTSmooth %>%
		purrr::map_dfr(., function(x)
			as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
		dplyr::bind_cols(date = c(ef$nc$bigTDates, ef$nc$bigTauDates) %>% .[1:(length(.) - 1)], .) 
	
	
	
	
	## Forecasting step
	zTCondBigT = list()
	sigmaZTCondBigT = list()
	yTCondBigT = list()
	sigmaYTCondBigT = list()
	
	for (j in 1:length(ef$nc$bigTStarDates)) {
		zTCondBigT[[j]] = bMat %*% {if (j == 1) zTCondT[[length(zTCondT)]] else zTCondBigT[[j - 1]]} + cMat
		sigmaZTCondBigT[[j]] = bMat %*% {if (j == 1) sigmaZTCondT[[length(sigmaZTCondT)]] else sigmaZTCondBigT[[j - 1]]} + qMat
		yTCondBigT[[j]] = aMat %*% zTCondBigT[[j]] + dMat
		sigmaYTCondBigT[[j]] = aMat %*% sigmaZTCondBigT[[j]] %*% t(aMat) + rMats[[1]]
	}
	
	kForecast =
		zTCondBigT %>%
		purrr::map_dfr(., function(x)
			as.data.frame(x) %>% t(.) %>% as_tibble(.)
			) %>%
		dplyr::bind_cols(date = ef$nc$bigTStarDates, .)
	

	
	# Plot and Cleaning
	kfPlots =
		lapply(colnames(ef$nc$zDf) %>% .[. != 'date'], function(.varname)
			dplyr::bind_rows(
				dplyr::mutate(ef$nc$zDf, type = 'Data'),
				dplyr::mutate(kFitted, type = 'Kalman Filtered'),
				dplyr::mutate(kForecast, type = 'Forecast'),
				dplyr::mutate(kSmooth, type = 'Kalman Smoothed'),
				) %>%
				tidyr::pivot_longer(., -c('date', 'type'), names_to = 'varname') %>%
				dplyr::filter(., varname == .varname) %>%
				ggplot(.) +
				geom_line(aes(x = date, y = value, color = type), size = 1) + 
				labs(x = NULL, y = NULL, color = NULL, title = paste0('Kalman smoothed values for ', .varname)) +
	            scale_x_date(date_breaks = '1 year', date_labels = '%Y') +
				ggthemes::theme_fivethirtyeight()
			)
	
	
	fDf =
		dplyr::bind_rows(
			kSmooth,
			tail(kFitted, 1),
			kForecast #%>% dplyr::mutate(., f1 = mean(dplyr::filter(ef$nc$zDf, date < '2020-03-01')$f1))
			)
	
	yDf =
		yTCondBigT %>%
		purrr::map_dfr(., function(x) as_tibble(t(as.data.frame(x)))) %>%
		dplyr::bind_cols(date = ef$nc$bigTStarDates, .)
	
	kfDf = yDf
		# yDf %>%
		# dplyr::select(
		# 	.,
		# 	date#,
		# 	#all_of(dplyr::filter(ef$paramsDf, nowcast == 'kf')$varname)
		# 	)
	
	ef$nc$fDf <<- fDf
	ef$nc$kfPlots <<- kfPlots
	ef$nc$yDf <<- yDf
	ef$nc$kfDf <<- kfDf
})


## ----------------------------------------------------------------
local({
  
	conn =
		dbConnect(
		RPostgres::Postgres(),
		dbname = CONST$DB_DATABASE,
		host = CONST$DB_SERVER,
		port = 5432,
		user = CONST$DB_USERNAME,
		password = CONST$DB_PASSWORD
		)
	
	reqs =
		tribble(
			~fcname, ~varname,
			'dns', 't10y',
			'dns', 't30y',
			'dns', 't05y',
			'dns', 't02y',
			'dns', 't01y',
			'dns', 't06m',
			'dns', 't03m',
			'dns', 't01m',
			'dns', 't20y',
			'dns', 't07y',
			'cme', 'ffr',
			'cme', 'sofr'
			)
	
	cmefiDf =
		DBI::dbGetQuery(conn, 'SELECT * FROM fc_forecast') %>%
		as_tibble(.) %>%
		dplyr::right_join(., reqs, by = c('fcname', 'varname')) %>%
		dplyr::group_by(., varname) %>%
		dplyr::filter(., vintage_date == max(vintage_date)) %>%
		dplyr::ungroup(.) %>%
		dplyr::select(., -vintage_date, -fcname) %>%
		dplyr::rename(., date = obs_date) %>%
		dplyr::mutate(., freq = 'm') %>%
		# Only keep rows without a match in FRED data
		dplyr::anti_join(., ef$h$source$fred, by = c('varname', 'date', 'freq')) %>%
		# Pivot
		dplyr::select(., -freq) %>%
		tidyr::pivot_wider(., names_from = varname, values_from = value) %>%
		dplyr::arrange(., date) %>%
		dplyr::filter(., date <= tail(ef$nc$bigTStarDates, 1))
	
	
  ef$nc$cmefiDf <<- cmefiDf
})


## ----------------------------------------------------------------
local({
	
	dfmVarnames = ef$paramsDf %>% dplyr::filter(., nowcast == 'dfm.m') %>% .$varname
	
	dfmMDf = 
		lapply(dfmVarnames, function(.varname) {
			inputDf =
				dplyr::inner_join(
					ef$nc$fDf,
					dplyr::select(ef$h$m$st, date, all_of(.varname)),
					by = 'date'
				) %>%
				na.omit(.)

			yMat = dplyr::select(inputDf, all_of(.varname)) %>% as.matrix(.)
			xDf = dplyr::select(inputDf, -date, -all_of(.varname)) %>% dplyr::mutate(., constant = 1)

			coefDf =
				lm(yMat ~ . - 1, xDf)$coef %>%
	    		as.data.frame(.) %>% rownames_to_column(., 'coefname')	%>% as_tibble(.) %>%
				setNames(., c('coefname', 'value'))

			# Forecast dates + last historical date
			forecastDf0 =
				tibble(
					date = seq(from = tail(inputDf$date, 1), to = tail(ef$nc$bigTStarDates, 1), by = '1 month')
					) %>%
				.[2:nrow(.), ] %>%
				dplyr::mutate(., !!.varname := 0) %>%
				dplyr::left_join(., ef$nc$fDf, by = 'date') %>%
				dplyr::bind_cols(., constant = 1)

			forecastDf =
				purrr::reduce(1:nrow(forecastDf0), function(accum, x) {
					accum[x, .varname] =
						accum[x, ] %>%
						dplyr::select(., coefDf$coefname) %>%
						matrix(., ncol = 1) %>%
						{matrix(coefDf$value, nrow = 1) %*% as.numeric(.)}

					return(accum)
				}, .init = forecastDf0) %>%
				dplyr::select(., all_of(c('date', .varname)))
# 
# 			inputDf =
# 				dplyr::inner_join(
# 				ef$nc$fDf,
# 				dplyr::select(ef$h$m$st, date, all_of(.varname)) %>%
# 					econforecasting::addLags(., 1, .zero = TRUE),
# 				by = 'date'
# 				) %>%
# 				na.omit(.)
# 			
# 			yMat = dplyr::select(inputDf, all_of(.varname)) %>% as.matrix(.)
# 			xDf = dplyr::select(inputDf, -date, -all_of(.varname)) %>% dplyr::mutate(., constant = 1)
# 			
# 			coefDf =
# 				lm(yMat ~ . - 1, xDf)$coef %>%
# 	    		as.data.frame(.) %>% rownames_to_column(., 'coefname')	%>% as_tibble(.) %>%
# 				setNames(., c('coefname', 'value'))
# 			
# 			# Forecast dates + last historical date
# 			forecastDf0 =
# 				tibble(
# 					date = seq(from = tail(inputDf$date, 1), to = tail(ef$nc$bigTStarDates, 1), by = '1 month')
# 					) %>%
# 				dplyr::mutate(., !!.varname := c(inputDf[[paste0(.varname, '.l1')]][[1]], rep(NA, nrow(.) - 1))) %>%
# 				dplyr::left_join(., ef$nc$fDf, by = 'date') %>%
# 				dplyr::bind_cols(., constant = 1)
# 			
# 			forecastDf =
# 				purrr::reduce(2:nrow(forecastDf0), function(accum, x) {
# 					accum[x, .varname] =
# 						accum %>% econforecasting::addLags(., 1, .zero = TRUE) %>%
# 						.[x, ] %>%
# 						dplyr::select(., coefDf$coefname) %>%
# 						matrix(., ncol = 1) %>%
# 						{matrix(coefDf$value, nrow = 1) %*% as.numeric(.)}
# 					
# 					return(accum)
# 				}, .init = forecastDf0) %>%
# 				.[2:nrow(.), ] %>%
# 				dplyr::select(., all_of(c('date', .varname)))
		
			}) %>%
			purrr::reduce(., function(accum, x) dplyr::full_join(accum, x, by = 'date')) %>%
			dplyr::arrange(., date)


	ef$nc$dfmMDf <<- dfmMDf	
})


## ----------------------------------------------------------------
local({
	
	dfmVarnames = ef$paramsDf %>% dplyr::filter(., nowcast == 'dfm.q') %>% .$varname
	
	fDf =
		ef$nc$fDf %>%
		tidyr::pivot_longer(., -date, names_to = 'varname') %>%
		dplyr::mutate(
			.,
			q = econforecasting::strdateToDate(paste0(lubridate::year(date), 'Q', lubridate::quarter(date)))
			) %>%
		dplyr::select(., -date) %>%
		dplyr::group_by(., varname, q) %>%
		dplyr::summarize(., value = mean(value)) %>%
		tidyr::pivot_wider(., names_from = varname, values_from = value) %>%
		dplyr::rename(., date = q)
	
	
	dfmRes =
		lapply(dfmVarnames %>% setNames(., .), function(.varname) {
			
	# 		inputDf =
	# 			dplyr::inner_join(
	# 				fDf,
	# 				dplyr::select(ef$h$q$st, date, all_of(.varname)),
	# 				by = 'date'
	# 			) %>%
	# 			na.omit(.)
	# 
	# 		yMat = dplyr::select(inputDf, all_of(.varname)) %>% as.matrix(.)
	# 		xDf = dplyr::select(inputDf, -date, -all_of(.varname)) %>% dplyr::mutate(., constant = 1)
	# 
	# 		coefDf =
	# 			lm(yMat ~ . - 1, xDf)$coef %>%
	#     		as.data.frame(.) %>% rownames_to_column(., 'coefname')	%>% as_tibble(.) %>%
	# 			setNames(., c('coefname', 'value'))
	# 
	# 		# Forecast dates + last historical date
	# 		forecastDf0 =
	# 			tibble(
	# 				date = seq(from = tail(inputDf$date, 1), to = tail(ef$nc$bigTStarDates, 1), by = '3 months')
	# 				) %>%
	# 			.[2:nrow(.), ] %>%
	# 			dplyr::mutate(., !!.varname := 0) %>%
	# 			dplyr::left_join(., ef$nc$fDf, by = 'date') %>%
	# 			dplyr::bind_cols(., constant = 1)
	# 
	# 		forecastDf =
	# 			purrr::reduce(1:nrow(forecastDf0), function(accum, x) {
	# 				accum[x, .varname] =
	# 					accum[x, ] %>%
	# 					dplyr::select(., coefDf$coefname) %>%
	# 					matrix(., ncol = 1) %>%
	# 					{matrix(coefDf$value, nrow = 1) %*% as.numeric(.)}
	# 
	# 				return(accum)
	# 			}, .init = forecastDf0) %>%
	# 			dplyr::select(., all_of(c('date', .varname)))

			inputDf =
				dplyr::inner_join(
					fDf,
					dplyr::select(ef$h$q$st, date, all_of(.varname)) %>%
						econforecasting::addLags(., 1, .zero = TRUE),
					by = 'date'
				) %>%
				na.omit(.)

			yMat = dplyr::select(inputDf, all_of(.varname)) %>% as.matrix(.)
			xDf = dplyr::select(inputDf, -date, -all_of(.varname)) %>% dplyr::mutate(., constant = 1)
			
			glmResult = lapply(c(1), function(.alpha) {
				cv =
					glmnet::cv.glmnet(
						x = xDf %>% dplyr::select(., -constant) %>% as.matrix(.),
						y = yMat,
						deviance = 'mae',
						alpha = .alpha,
						intercept = TRUE
					)
				tibble(alpha = .alpha, lambda = cv$lambda, mae = cv$cvm) %>%
				dplyr::mutate(., min_lambda_for_given_alpha = (mae == min(mae))) %>%
				return(.)
				}) %>%
				dplyr::bind_rows(.) %>%
				dplyr::mutate(., min_overall = (mae == min(mae)))

			glmOptim = glmResult %>% dplyr::filter(., min_overall == TRUE)

			cvPlot =
				glmResult %>%
				ggplot(.) +
				geom_line(aes(x = log(lambda), y = mae, group = alpha, color = alpha)) +
				geom_point(
					data = glmResult %>% dplyr::filter(., min_lambda_for_given_alpha == TRUE),
					aes(x = log(lambda), y = mae), color = 'red'
				) +
				geom_point(
					data = glmResult %>% dplyr::filter(., min_overall == TRUE),
					aes(x = log(lambda), y = mae), color = 'green'
				) +
				labs(
					x = 'log(Lambda)', y = 'MAE', color = 'alpha',
					title = 'Elastic Net Hyperparameter Fit',
					subtitle = 'Red = MAE Minimizing Lambda for Given Alpha;
					Green = MAE Minimizing (Lambda, Alpha) Pair'
					)
			
		
			glmObj =
				glmnet::glmnet(
					x = xDf %>% dplyr::select(., -constant) %>% as.matrix(.),
					y = yMat,
					alpha = glmOptim$alpha,
					lambda = glmOptim$lambda
				)
  
			coefMat = glmObj %>% coef(.) %>% as.matrix(.)
			
			coefDf =
				coefMat %>%
			    as.data.frame(.) %>%
			    rownames_to_column(., var = 'Covariate') %>%
			    setNames(., c('coefname', 'value')) %>%
				as_tibble(.) %>%
				dplyr::mutate(., coefname = ifelse(coefname == '(Intercept)', 'constant', coefname))

			
			# Standard OLS
	# 		coefDf =
	# 			lm(yMat ~ . - 1, xDf)$coef %>%
	#     		as.data.frame(.) %>% rownames_to_column(., 'coefname') %>% as_tibble(.) %>%
	# 			setNames(., c('coefname', 'value'))

			# Forecast dates + last historical date
			forecastDf0 =
				tibble(
					date = seq(from = tail(inputDf$date, 1), to = tail(ef$nc$bigTStarDates, 1), by = '3 months')
					) %>%
				dplyr::mutate(., !!.varname := c(tail(inputDf[[.varname]], 1), rep(NA, nrow(.) - 1))) %>%
				dplyr::left_join(., ef$nc$fDf, by = 'date') %>%
				dplyr::bind_cols(., constant = 1)

			forecastDf =
				purrr::reduce(2:nrow(forecastDf0), function(accum, x) {
					accum[x, .varname] =
						accum %>% econforecasting::addLags(., 1, .zero = TRUE) %>%
						.[x, ] %>%
						dplyr::select(., coefDf$coefname) %>%
						matrix(., ncol = 1) %>%
						{matrix(coefDf$value, nrow = 1) %*% as.numeric(.)}

					return(accum)
				}, .init = forecastDf0) %>%
				.[2:nrow(.), ] %>%
				dplyr::select(., all_of(c('date', .varname)))
			
			list(
				forecastDf = forecastDf,
				coefDf = coefDf,
				glmOptim = glmOptim,
				cvPlot = cvPlot
				)
			})
	
	
	glmCoefList = purrr::map(dfmRes, ~ .$coefDf) 
	
	glmOptimDf =
		purrr::imap_dfr(dfmRes, function(x, i)
			x$glmOptim %>% dplyr::bind_cols(varname = i, .)
			)
	
	cvPlots = purrr::map(dfmRes, ~ .$cvPlot)
	
	dfmDf =
		dfmRes %>%
		lapply(., function(x) x$forecastDf) %>%
		purrr::reduce(., function(accum, x) dplyr::full_join(accum, x, by = 'date')) %>%
		dplyr::arrange(., date)


	ef$nc$glmCoefQList <<-glmCoefList 
	ef$nc$glmOptimQDf <<- glmOptimDf
	ef$nc$cvPlotsQ <<- cvPlots
	ef$nc$dfmQDf <<- dfmDf
})


## ----------------------------------------------------------------
local({
	
	mDf =
		list(ef$nc$dfmMDf, ef$nc$cmefiDf) %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)
	
	qDf =
		list(ef$nc$dfmQDf) %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)


	ef$ncpred0$m$st <<- mDf
	ef$ncpred0$q$st <<- qDf
})


## ----------------------------------------------------------------
local({
	
	mDf =
		ef$ncpred0$m$st %>%
		{lapply(colnames(.) %>% .[. != 'date'], function(.varname) {
			
			transform = dplyr::filter(ef$paramsDf, varname == .varname)$st
			
			fcDf = dplyr::select(., date, .varname) %>% na.omit(.)
				
			histDf = tail(dplyr::filter(na.omit(ef$h$m$ut[, c('date', .varname)]), date < min(fcDf$date)), 1)
			
			fcDf %>%
				dplyr::mutate(
					.,
					!!.varname := 
						{
	                        if (transform == 'dlog') undlog(fcDf[[2]], histDf[[2]])
							else if (transform == 'base') .[[2]]
							else stop('Err: ', .varname)
							}
						)
			
			})} %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)
	
	
	qDf =
		ef$ncpred0$q$st %>%
		{lapply(colnames(.) %>% .[. != 'date'], function(.varname) {
			
			transform = dplyr::filter(ef$paramsDf, varname == .varname)$st
			
			fcDf = dplyr::select(., date, .varname) %>% na.omit(.)
				
			histDf = tail(dplyr::filter(na.omit(ef$h$q$ut[, c('date', .varname)]), date < min(fcDf$date)), 1)
			
			fcDf %>%
				dplyr::mutate(
					.,
					!!.varname := 
						{
	                        if (transform == 'dlog') undlog(fcDf[[2]], histDf[[2]])
							else if (transform == 'base') .[[2]]
							else stop('Err: ', .varname)
							}
						)
			
			})} %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)
	

	ef$ncpred0$m$ut <<- mDf
	ef$ncpred0$q$ut <<- qDf
})


## ----------------------------------------------------------------
local({
	
	qDf =
		ef$ncpred0$q$ut %>%
		dplyr::transmute(
			.,
			date,
			govt = govtf + govts,
			ex = exg + exs,
			im = img + ims,
			nx = ex - im,
			pdin = pdinstruct + pdinequip + pdinip,
			pdi = pdin + pdir + pceschange,
			pces = pceshousing + pceshealth + pcestransport + pcesrec + pcesfood + pcesfinal + pcesother + pcesnonprofit,
			pcegn = pcegnfood + pcegnclothing + pcegngas + pcegnother,
			pcegd = pcegdmotor + pcegdfurnish + pcegdrec + pcegdother,
			pceg = pcegn + pcegd,
			pce = pceg + pces,
			gdp = pce + pdi + nx + govt
		)
	
	mDf =
		ef$ncpred0$m$ut %>%
		dplyr::transmute(
			.,
			date,
			psr = ps/pid
		)
	
	ef$ncpred1$m$ut <<- mDf
	ef$ncpred1$q$ut <<- qDf
})


## ----------------------------------------------------------------
local({
	
	wide = list()
	
	wide$m$ut =
		list(ef$ncpred0$m$ut, ef$ncpred1$m$ut) %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)

	wide$q$ut =
		list(ef$ncpred0$q$ut, ef$ncpred1$q$ut) %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)

	
	wide$m$d1 =
		wide$m$ut %>%
		{lapply(colnames(.) %>% .[. != 'date'], function(.varname) {
			
			transform = dplyr::filter(ef$paramsDf, varname == .varname)$d1
			fcDf = dplyr::select(., date, .varname) %>% na.omit(.)
			
			histDf = tail(dplyr::filter(na.omit(ef$h$m$ut[, c('date', .varname)]), date < min(fcDf$date)), 1)
			
			dplyr::bind_rows(histDf, fcDf) %>%
				dplyr::mutate(
					.,
					!!.varname := 
						{
	                        if (transform == 'apchg') apchg(.[[2]], 12)
							else if (transform == 'base') .[[2]]
							else stop('Err: ', .varname)
							}
						) %>%
				.[2:nrow(.), ]
			
			})} %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)

	
	wide$q$d1 =
		wide$q$ut %>%
		{lapply(colnames(.) %>% .[. != 'date'], function(.varname) {
			
			transform = dplyr::filter(ef$paramsDf, varname == .varname)$d1
			fcDf = dplyr::select(., date, .varname) %>% na.omit(.)
			
			histDf = tail(dplyr::filter(na.omit(ef$h$q$ut[, c('date', .varname)]), date < min(fcDf$date)), 1)
			
			dplyr::bind_rows(histDf, fcDf) %>%
				dplyr::mutate(
					.,
					!!.varname := 
						{
	                        if (transform == 'apchg') apchg(.[[2]], 4)
							else if (transform == 'base') .[[2]]
							else stop('Err: ', .varname)
							}
						) %>%
				.[2:nrow(.), ]
			
			})} %>%
		purrr::reduce(., function(x, y) dplyr::full_join(x, y, by = 'date')) %>%
		dplyr::arrange(., date)

	
	ef$ncpred <<- wide
})


## ----------------------------------------------------------------
local({
	
	flat =
		ef$ncpred %>%
		purrr::imap_dfr(function(x, .freq)
			x %>% purrr::imap_dfr(function(df, .form)
				df %>%
					tidyr::pivot_longer(., -date, names_to = 'varname', values_to = 'value') %>%
					dplyr::mutate(., freq = .freq, form = .form, vdate = VINTAGE_DATE)
				)
			) %>%
		na.omit(.)
						
	ef$ncpredFlat <<- flat
})


## ----------------------------------------------------------------
local({
	
	conn =
		dbConnect(
		RPostgres::Postgres(),
		dbname = CONST$DB_DATABASE,
		host = CONST$DB_SERVER,
		port = 5432,
		user = CONST$DB_USERNAME,
		password = CONST$DB_PASSWORD
		)
	
	# TRUNCATE nc_releases;TRUNCATE nc_params CASCADE;TRUNCATE nc_values;TRUNCATE nc_history;
	if (RESET_ALL == TRUE) {
		DBI::dbGetQuery(conn, 'TRUNCATE nc_params CASCADE')
		DBI::dbGetQuery(conn, 'TRUNCATE nc_values')
		DBI::dbGetQuery(conn, 'TRUNCATE nc_history')
		DBI::dbGetQuery(conn, 'TRUNCATE nc_releases')
	}
	
	# Send variables to SQL
	query =
	  econforecasting::createInsertQuery(
	    ef$h$flatDf %>% dplyr::mutate(value = round(value, 4)),
	    'nc_history',
	    'ON CONFLICT ON CONSTRAINT nc_history_varname_date_form_freq DO UPDATE SET value = EXCLUDED.value'
	    )
	res = DBI::dbGetQuery(conn, query)

	
	# Send variables to SQL
	query =
	  econforecasting::createInsertQuery(
	    ef$paramsDf %>% dplyr::select(., -releaseid),
	    'nc_params',
	    'ON CONFLICT ON CONSTRAINT nc_params_varname DO UPDATE SET fullname=EXCLUDED.fullname,
	    category=EXCLUDED.category, dispgroup=EXCLUDED.dispgroup, source=EXCLUDED.source,
	    sckey=EXCLUDED.sckey, units=EXCLUDED.units, freq=EXCLUDED.freq, sa=EXCLUDED.sa,
	    st=EXCLUDED.st, d1=EXCLUDED.d1, d2=EXCLUDED.d2, dfminput=EXCLUDED.dfminput,
	    nowcast=EXCLUDED.nowcast'
	    )
	res = DBI::dbGetQuery(conn, query)

	
	# Insert nc_values
	query =
	  econforecasting::createInsertQuery(
	    ef$ncpredFlat,
	    'nc_values',
	    'ON CONFLICT ON CONSTRAINT nc_values_varname_date_form_vdate_freq DO UPDATE SET value = EXCLUDED.value'
	    )
	res = DBI::dbGetQuery(conn, query)
	
	
	# Insert nc_releases
	query =
	  econforecasting::createInsertQuery(
	    ef$releaseDf,
	    'nc_releases',
	    'ON CONFLICT ON CONSTRAINT nc_releases_id DO UPDATE SET link=EXCLUDED.link, count=EXCLUDED.count,seriesnames=EXCLUDED.seriesnames,reldates=EXCLUDED.reldates'
	    )
	res = DBI::dbGetQuery(conn, query)

	
	# Insert fc_forecast_last_vintage
	# forecastLastVintageDf =
	# 	ef$forecastDf %>%
	# 	dplyr::group_by(fcname) %>% 
	# 	dplyr::filter(., vintageDate == max(vintageDate)) %>%
	# 	dplyr::ungroup(.) %>%
	# 	dplyr::rename(., obs_date = date, vintage_date = vintageDate)
	# query =
	#   econforecasting::createInsertQuery(
	#     forecastLastVintageDf,
	#     'fc_forecast_last_vintage',
	#     'ON CONFLICT ON CONSTRAINT fc_forecast_last_vintage_fcname_obs_date_varname DO UPDATE SET value = EXCLUDED.value, vintage_date = EXCLUDED.vintage_date'
	#     )
	# res = DBI::dbGetQuery(conn, query)
	
})


## ----------------------------------------------------------------
local({
	

	basename =  paste0(VINTAGE_DATE, '-nowcast')
	saveRDS(list(CONST = CONST, ef = ef), file = file.path(OUTPUT_DIR, paste0(basename, '.rds')))
	# setwd(str_replace(DOC_PATH, '/[^/]+$', ''))
	
	# Cleanup existing .pdf and .tex files
	lapply(list.files(path = OUTPUT_DIR, pattern = '\\.tex$', full.names = TRUE), function(x) file.remove(x))
	lapply(list.files(path = OUTPUT_DIR, pattern = '\\.pdf$', full.names = TRUE), function(x) file.remove(x))
	unlink(file.path(OUTPUT_DIR, 'latex-figures'), recursive = TRUE)
	
	# Cleanup latex-figures doc
	
	if (!is.null(DOC_DIR)) {	
		knitr::knit2pdf(
			input = file.path(DOC_DIR, 'nowcast.rnw'),
			output = file.path(OUTPUT_DIR, paste0(basename, '.tex')),
			clean = TRUE
			)
		
		file.remove(file.path(OUTPUT_DIR, paste0(basename, '.log')))

		file.copy(
			from =  file.path(OUTPUT_DIR, paste0(basename, '.pdf')),
			to = file.path(DIR, 'nowcast-documentation.pdf'),
			overwrite = TRUE
			)

		file.copy(
			from =  file.path(OUTPUT_DIR, paste0(basename, '.pdf')),
			to = file.path(DOC_DIR, 'nowcast-documentation.pdf'),
			overwrite = TRUE
			)

	}
})

