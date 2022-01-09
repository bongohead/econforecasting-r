# Initialize ----------------------------------------------------------

## Load Libs ----------------------------------------------------------'
library(tidyverse)
library(httr)

## List of Stocks ----------------------------------------------------------
stocks = c(
	'THI', 'HD', 'CHWY', 'CCL', 'TGT', 'WMT', 'AMZN', 'AAPL',
	'TSLA', 'NKE', 'GOOGL', 'ZD', 'Z', 'BAC', 'SNV', 'GPN',
	'NYT', 'FB', 'MSFT', 'NFLX', 'KSS', 'KO', 'PEP', 'MCD',
	'YUM', 'C', 'M', 'X', 'DAL', 'AAL', 'SPY', 'CMG', 'PZZA', 
	'DPZ'
	)

## Yahoo Finance ----------------------------------------------------------
local({
	
	message('***** Importing Yahoo Finance Data')
	
	yahoo_data_raw =
		stocks %>%
		lapply(., function(x) {
			
			message(x)
			
			url = paste0(
				'https://query1.finance.yahoo.com/v7/finance/download/', x,
				'?period1=', '1262304000', # 12/30/1999
				'&period2=', as.numeric(as.POSIXct(Sys.Date() + lubridate::days(1))),
				'&interval=1d',
				'&events=history&includeAdjustedClose=true'
				)
			
			data.table::fread(url, showProgress = TRUE) %>%
				.[, c('Date', 'Adj Close')]	%>%
				set_names(., c('date', 'value')) %>%
				as_tibble(.) %>%
				# Bug with yahoo finance returning null for date 7/22/21 as of 7/23
				filter(., value != 'null') %>%
				mutate(
					.,
					ticker = x,
					date,
					value = as.numeric(value)
				) %>%
				return(.)
		}) %>%
		bind_rows(.)
	
	
	ticker_starts =
		yahoo_data_raw %>%
		group_by(., ticker) %>%
		summarize(., min_date = min(date)) %>%
		arrange(., desc(min_date))
		
	yahoo_data =
		yahoo_data_raw %>%
		filter(., !ticker %in% c('CHWY', 'THI', 'Z')) %>%
		pivot_wider(., id_cols = date, names_from = ticker, values_from = value) %>%
		arrange(., date) %>%
		na.omit(.)
	
	final_data =
		yahoo_data %>%
		mutate(across(-date, function(x) (x - lag(x, 1))/lag(x, 1) * 100)) %>%
		na.omit(.) %>%
		mutate(across(-date, function(x) zoo::rollmean(x, 7, na.pad = T, align = 'right'))) %>%
		na.omit(.)
	
	final_long = final_data %>% pivot_longer(., cols = -c('date'), names_to = 'name', values_to = 'value')
	final_long %>% ggplot(aes(x = date, y = value, color = name)) + geom_line()

	highchart(type = 'stock') %>%
		hc_add_series(filter(final_long, name == stocks[2]) %>% select(., name, value), type = 'line')

	 
})





