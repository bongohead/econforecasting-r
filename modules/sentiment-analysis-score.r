#' Sentiment Analysis with Dictionary Analysis & BERT

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-score'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = TRUE

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
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)
library(tidytext)
library(reticulate)
use_virtualenv(file.path(EF_DIR, '.virtualenvs', 'econforecasting'))

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

## BERT Analysis --------------------------------------------------------
happytransformer = import('happytransformer')
happy_tc = happytransformer$HappyTextClassification(
	'DISTILBERT', # 'ROBERTA',
	'distilbert-base-uncased-finetuned-sst-2-english', # 'siebert/sentiment-roberta-large-english',
	num_labels = 2
)

fwrite(set_names(pulled_data[, 'title'], 'text'), file.path(tempdir(), 'text.csv'))
classified_text = happy_tc$test(file.path(tempdir(), 'text.csv'))

bert_result = data.table(
	bert_label = map_chr(classified_text, ~ .$label),
	bert_score = map_chr(classified_text, ~ .$score)
)[, bert_label := ifelse(bert_label == 'POSITIVE', 1, -1)]


## Dictionary Analysis --------------------------------------------------------
dict =
	list(
		get_sentiments('nrc') %>%
			as.data.table(.) %>%
			.[, sentiment := fcase(
				sentiment %chin% c('trust', 'positive', 'joy', 'anticipation', 'surprise'), 1,
				sentiment %chin% c('disgust', 'sadness', 'fear', 'negative', 'anger'), -1
			)],
		get_sentiments('bing') %>%
			as.data.table(.) %>%
			.[, sentiment := ifelse(sentiment == 'positive', 1, -1)]
	) %>%
	rbindlist(.) %>%
	.[, list(sentiment = head(sentiment, 1)), by = 'word']

dict_result =
	pulled_data %>%
	as.data.table(.) %>%
	.[, c('name', 'title')] %>% # Name is the unique post identifier
	tidytext::unnest_tokens(., word, title) %>%
	.[!word %chin% stop_words$word]  %>%
	merge(., dict, by = 'word', all = F, allow.cartesian = T) %>%
	.[, list(dict_score = sum(sentiment)/.N), by = 'name'] %>%
	.[, dict_score := ifelse(dict_score >= 0, 1, -1)] 

## Score Analysis --------------------------------------------------------
scored_data =
	pulled_data %>%
	merge(., dict_result, all.x = T) %>%
	bind_cols(., bert_result[, c('bert_label', 'bert_score')])


scored_data %>%
	.[, list(score = mean(bert_label, na.rm = T)), by = c('created_date', 'subreddit')] %>%
	.[order(subreddit, created_date)] %>%
	.[, score := frollmean(score, n = 30, fill = NA), by = 'subreddit'] %>%
	ggplot(.) +
	geom_line(aes(x = created_date, y = score, color = subreddit))

# .[, created_dttm := as_datetime(created)] %>%
# 	.[, created_date := as_date(created_dttm)] %>%

#%>%
# dplyr::left_join(
# 	.,
# 	dplyr::group_by(., date) %>%
# 		dplyr::summarize(., dateTotal = n(), .groups = 'drop'),
# 	by = 'date'
# ) %>%
# dplyr::group_by(., sentiment, date) %>%
# dplyr::summarize(., count = n(), dateTotal = unique(dateTotal), .groups = 'drop') %>%
# dplyr::mutate(., percent = count/dateTotal) %>%
# ggplot(.) + geom_line(aes(x = date, y = percent, color = sentiment, group = sentiment))


# Pull Data

## Web Scrape
local({
	
	sentMeta = DBI::dbGetQuery(db, 'SELECT * FROM sent_meta') %>% as_tibble(.)
	
	rawDf =
		# Iterate through pages
		# Go up to 3000
		purrr::reduce(1:3000, function(accum, page) {
			#if (page %% 20 == 1)
			message('Downloading data for page ', page)
			pageContent =
				httr::GET(paste0('https://www.reuters.com/news/archive/businessnews?view=page&page=',
												 page, '&pageSize=10')) %>%
				httr::content(.) %>%
				rvest::html_node(., 'div.column1')
			
			res =
				tibble(
					page = page,
					article1 = html_text(html_nodes(pageContent, 'h3.story-title'), trim = TRUE),
					article2 = html_text(html_nodes(pageContent, 'div.story-content > p'), trim = TRUE),
					date = html_text(html_nodes(pageContent, 'span.timestamp'), trim = TRUE)
				) %>%
				mutate(
					.,
					date = ifelse(str_detect(date, coll('EDT')) == TRUE, format(Sys.Date(), '%b %d %Y'), date),
					date = as.Date(parse_date_time2(date, '%b %d %Y'))
				) %>%
				bind_rows(accum, .)
			
			if (any(as.Date(res$date) %in% sentMeta$date)) return(done(res))
			return(res)
		}, .init = tibble()) %>%
		dplyr::filter(., date < as.Date(Sys.Date()) & !date %in% sentMeta$date)
	
	
	rawDf <<- rawDf
})


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
# })
