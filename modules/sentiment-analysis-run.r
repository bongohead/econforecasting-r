#' Reddit scraper (sentiment analysis)
#' 
#' Chek curl example for correct headers to send: https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example
#' After token is fetched: https://www.reddit.com/dev/api
#' Page scraping query arguments: https://www.reddit.com/dev/api#listings

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-run'
EF_DIR = Sys.getenv('EF_DIR')

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------'
library(econforecasting)
library(tidyverse)
library(data.table)
library(httr)
library(RCurl)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)
library(tidytext)

## Load Connection Info ----------------------------------------------------------
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)

# Get Data ----------------------------------------------------------------

## Reddit Token --------------------------------------------------------
token = 
	POST(
		'https://www.reddit.com/api/v1/access_token',
		add_headers(c(
			'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
			'Authorization' = paste0(
				'Basic ', base64(txt = paste0(CONST$REDDIT_ID, ':', CONST$REDDIT_SECRET), mode = 'character')
				)
			)),
		body = list(grant_type = 'client_credentials', username = CONST$REDDIT_USERNAME, password = CONST$REDDIT_PASSWORD),
		encoding = 'json'
		) %>%
	httr::content(., 'parsed') %>%
	.$access_token

## Pull Data --------------------------------------------------------
pulled_data = accumulate(1:100, function(accum, i) {
	
	message('***** Pull ', i, ' of 100')
	query = list(
		t = 'month',
		# g = 'GLOBAL',
		limit = 100,
		show = 'all',
		after = {if (i == 1) NULL else tail(accum, 1)$after}
		) %>%
		compact(.) %>%
		paste0(names(.), '=', .) %>%
		paste0(collapse = '&')
	
	message(query)
	
	http_result =
		GET(
			paste0('https://oauth.reddit.com/r/all/top?', query),
			add_headers(c(
				'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
				'Authorization' = paste0('bearer ', token)
				))
			)
	
	calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
	reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
	print(calls_remaining)
	if (calls_remaining == 0) Sys.sleep(reset_seconds)
	message(str_glue('Calls Remaining: {calls_remaining} | Seconds to Reset: {reset_seconds}s'))

	result = content(http_result, 'parsed')
	if (is.null(result$data$after)) stop('Missing after')
	
	parsed =
		lapply(result$data$children, function(y) 
			y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
		) %>%
		rbindlist(., fill = T) %>%
		select(
			.,
			name, # Unique identifier
			subreddit, selftext, title, upvote_ratio, ups,
			score, post_hint, is_self, created, domain, url_overridden_by_dest
			) %>%
		as.data.table(.) %>%
		.[, c('i', 'after') := list(i, result$data$after)]

	return(rbindlist(list(accum, parsed)))
	}, .init = data.table()) %>%
	rbindlist(.)

# Verify no duplicated after posts
pulled_data %>%
	group_by(., i) %>%
	summarize(., uniq_after = paste0(unique(after), collapse = ','))

## Analysis --------------------------------------------------------
dict =
	get_sentiments('nrc') %>%
	as.data.table(.) %>%
	.[, sentiment := fcase(
		sentiment %chin% c('trust', 'positive', 'joy', 'anticipation', 'surprise'), 1,
		sentiment %chin% c('disgust', 'sadness', 'fear', 'negative', 'anger'), 0,
		default = 0
		)]

pulled_data %>%
	as.data.table(.) %>%
	.[, created_dttm := as_datetime(created)] %>%
	.[, created_date := as_date(created_dttm)] %>%
	.[, c('created_dttm', 'created_date', 'subreddit', 'domain', 'title')] %>%
	tidytext::unnest_tokens(., word, title) %>%
	.[!word %chin% stop_words$word]  %>%
	merge(., dict, by = 'word', all = F, allow.cartesian = T) %>%
	.[, list(sentiment_score = sum(sentiment)/.N), by =
# 		dplyr::left_join(
# 			.,
# 			dplyr::group_by(., date) %>%
# 				dplyr::summarize(., dateTotal = n(), .groups = 'drop'),
# 			by = 'date'
# 		) %>%
# 		dplyr::group_by(., sentiment, date) %>%
# 		dplyr::summarize(., count = n(), dateTotal = unique(dateTotal), .groups = 'drop') %>%
# 		dplyr::mutate(., percent = count/dateTotal) %>%
# 		ggplot(.) + geom_line(aes(x = date, y = percent, color = sentiment, group = sentiment))


# Pull Data

## Web Scrape
# local({
# 	
# 	sentMeta = DBI::dbGetQuery(db, 'SELECT * FROM sent_meta') %>% as_tibble(.)
# 	
# 	rawDf =
# 		# Iterate through pages
# 		# Go up to 3000
# 		purrr::reduce(1:3000, function(accum, page) {
# 			#if (page %% 20 == 1)
# 			message('Downloading data for page ', page)
# 			pageContent =
# 				httr::GET(paste0('https://www.reuters.com/news/archive/businessnews?view=page&page=', page, '&pageSize=10')) %>%
# 				httr::content(.) %>%
# 				rvest::html_node(., 'div.column1')
# 			
# 			res =
# 				tibble(
# 					page = page,
# 					article1 = html_text(html_nodes(pageContent, 'h3.story-title'), trim = TRUE),
# 					article2 = html_text(html_nodes(pageContent, 'div.story-content > p'), trim = TRUE),
# 					date = html_text(html_nodes(pageContent, 'span.timestamp'), trim = TRUE)
# 				) %>%
# 				mutate(
# 					.,
# 					date = ifelse(str_detect(date, coll('EDT')) == TRUE, format(Sys.Date(), '%b %d %Y'), date),
# 					date = as.Date(parse_date_time2(date, '%b %d %Y'))
# 				) %>%
# 				bind_rows(accum, .)
# 			
# 			if (any(as.Date(res$date) %in% sentMeta$date)) return(done(res))
# 			return(res)
# 		}, .init = tibble()) %>%
# 		dplyr::filter(., date < as.Date(Sys.Date()) & !date %in% sentMeta$date)
# 	
# 	
# 	rawDf <<- rawDf
# })


## Data to SQL
# local({
# 	
# 	
# 	rowsAdded =
# 		rawDf %>%
# 		transmute(., date, page, article1, article2) %>%
# 		mutate(., split = ceiling((1:nrow(.))/100)) %>% # Sent 100 max data points at a time
# 		group_by(., split) %>%
# 		group_split(., .keep = FALSE) %>%
# 		imap(., function(x, i) {
# 			message('Sending data to SQL ... ', i)
# 			econforecasting::createInsertQuery(x, 'sent_data') %>%
# 				DBI::dbExecute(db, .)
# 		}) %>% unlist(.) %>% {if (any(is.null(.))) stop('SQL Error!') else sum(.)}
# 	print(rowsAdded)
# })




## Text Analysis
# local({
# 	
# 	
# 	emotionsPlot =
# 		rawDf %>%
# 		dplyr::mutate(
# 			.,
# 			article = paste0(article1, ' ', article2),
# 			date = ifelse(str_detect(date, coll('EDT')) == TRUE, format(Sys.Date(), '%b %d %Y'), date),
# 			date = as.Date(parse_date_time2(date, '%b %d %Y'))
# 		) %>%
# 		tidytext::unnest_tokens(., word, article) %>%
# 		dplyr::anti_join(., stop_words, by = 'word') %>%
# 		dplyr::inner_join(
# 			.,
# 			get_sentiments('nrc') %>%
# 				dplyr::mutate(
# 					sentiment = ifelse(sentiment == 'disgust', 'negative', sentiment),
# 					sentiment = ifelse(sentiment == 'sadness', 'negative', sentiment),
# 					sentiment = ifelse(sentiment == 'disgust', 'negative', sentiment),
# 					sentiment = ifelse(sentiment == 'anticipation', 'neutral', sentiment),
# 				),
# 			by = 'word'
# 		) %>%
# 		dplyr::left_join(
# 			.,
# 			dplyr::group_by(., date) %>%
# 				dplyr::summarize(., dateTotal = n(), .groups = 'drop'),
# 			by = 'date'
# 		) %>%
# 		dplyr::group_by(., sentiment, date) %>%
# 		dplyr::summarize(., count = n(), dateTotal = unique(dateTotal), .groups = 'drop') %>%
# 		dplyr::mutate(., percent = count/dateTotal) %>%
# 		ggplot(.) + geom_line(aes(x = date, y = percent, color = sentiment, group = sentiment))
# 	
# 	rawDf %>%
# 		dplyr::mutate(
# 			.,
# 			article = paste0(article1, ' ', article2),
# 			date = ifelse(str_detect(date, coll('EDT')) == TRUE, format(Sys.Date(), '%b %d %Y'), date),
# 			date = as.Date(parse_date_time2(date, '%b %d %Y'))
# 		) %>%
# 		tidytext::unnest_tokens(., word, article) %>%
# 		dplyr::anti_join(., stop_words, by = 'word') %>%
# 		dplyr::inner_join(
# 			.,
# 			get_sentiments('nrc') %>%
# 				dplyr::mutate(
# 					sentiment = case_when(
# 						sentiment == 'disgust' ~ -1,
# 						sentiment == 'sadness' ~ -1,
# 						sentiment == 'anticipation' ~ 0,
# 						sentiment == 'anger' ~ -2,
# 						sentiment == 'fear' ~ -2,
# 						sentiment == 'joy' ~ 1,
# 						sentiment == 'surprise' ~ 0,
# 						sentiment == 'trust' ~ 1,
# 						sentiment == 'positive' ~ 1,
# 						sentiment == 'negative' ~ -1,
# 						sentiment == 'neutral' ~ 0,
# 						TRUE ~ 0
# 					)
# 				),
# 			by = 'word') %>%
# 		dplyr::left_join(
# 			.,
# 			dplyr::group_by(., date) %>%
# 				dplyr::summarize(., dateTotal = n(), .groups = 'drop'),
# 			by = 'date'
# 		) %>%
# 		dplyr::group_by(., sentiment, date) %>%
# 		dplyr::summarize(., count = n(), dateTotal = unique(dateTotal), .groups = 'drop') %>%
# 		dplyr::mutate(., percent = count/dateTotal) %>%
# 		dplyr::mutate(., weightedSentiment = sentiment * percent) %>%
# 		dplyr::group_by(date) %>%
# 		dplyr::summarize(., sentimentIndex = sum(weightedSentiment), .groups = 'drop') %>%
# 		dplyr::mutate(., sentimentIndexLagged = roll::roll_mean(sentimentIndex, 7)) %>%
# 		ggplot(.) +
# 		geom_line(aes(x = date, y = sentimentIndexLagged))
# 	
# 	
# 	# Benfords Law
# 	rawDf %>%
# 		dplyr::mutate(
# 			.,
# 			article = paste0(article1, ' ', article2),
# 			date = ifelse(str_detect(date, coll('EDT')) == TRUE, format(Sys.Date(), '%b %d %Y'), date),
# 			date = as.Date(parse_date_time2(date, '%b %d %Y'))
# 		) %>%
# 		tidytext::unnest_tokens(., word, article) %>% dplyr::filter(., str_detect(word, '^[0-9]*$')) %>% dplyr::mutate(., start = str_sub(word, 1, 1)) %>% dplyr::group_by(., start) %>% dplyr::summarize(., n = n()) %>% ggplot(.) + geom_col(aes(x = start, y = n))
# })
