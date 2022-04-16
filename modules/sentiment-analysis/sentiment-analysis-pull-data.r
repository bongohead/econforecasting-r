#' Scrape Text for Sentiment Analysis
#' 
#' For Reddit:
#' - Check curl example for correct headers to send: 
#' - https://github.com/reddit-archive/reddit/wiki/OAuth2-Quick-Start-Example
#' - After token is fetched: https://www.reddit.com/dev/api
#' - Page scraping query arguments: https://www.reddit.com/dev/api#listings

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-pull-data'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE && rstudioapi::isAvailable(child_ok = T) == F) {
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
library(rvest)
library(RCurl)
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)

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


# Reddit ----------------------------------------------------------------

## Reset SQL  ----------------------------------------------------------------
local({
if (RESET_SQL) {
		
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_scrape_reddit CASCADE')

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_scrape_reddit (
		id SERIAL PRIMARY KEY,
		method VARCHAR(255) NOT NULL,
		name VARCHAR(255) NOT NULL,
		subreddit VARCHAR(255) NOT NULL,
		title TEXT NOT NULL,
		created_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
		scraped_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
		selftext TEXT,
		upvote_ratio NUMERIC(4, 2),
		ups NUMERIC(20, 0),
		is_self BOOLEAN,
		domain TEXT,
		url_overridden_by_dest TEXT,
		UNIQUE (method, name)
		)'
	)
		
}
})

## Token --------------------------------------------------------
local({
	token =
		POST(
			'https://www.reddit.com/api/v1/access_token',
			add_headers(c(
				'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
				'Authorization' = paste0(
					'Basic ', base64(txt = paste0(CONST$REDDIT_ID, ':', CONST$REDDIT_SECRET), mode = 'character')
					)
				)),
			body = list(
				grant_type = 'client_credentials',
				username = CONST$REDDIT_USERNAME,
				password = CONST$REDDIT_PASSWORD
				),
			encoding = 'json'
			) %>%
		httr::content(., 'parsed') %>%
		.$access_token
	
	reddit <<- list()
	reddit$token <<- token
})

## Top (All) --------------------------------------------------------
local({
	
	message(str_glue('*** Pulling Top All: {format(now(), "%H:%M")}'))
	
	top_1000_today_all = reduce(1:9, function(accum, i) {
		
		query =
			list(t = 'day', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
			compact(.) %>%
			paste0(names(.), '=', .) %>%
			paste0(collapse = '&')
	
		http_result = GET(
			paste0('https://oauth.reddit.com/top?', query),
			add_headers(c(
				'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
				'Authorization' = paste0('bearer ', reddit$token)
				))
			)
		
		calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
		reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
		if (calls_remaining == 0) Sys.sleep(reset_seconds)
		result = content(http_result, 'parsed')
	
		parsed =
			lapply(result$data$children, function(y) 
				y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
			) %>%
			bind_rows(.) %>%
			select(., any_of(c(
				'name', 'subreddit', 'title', 'created',
				'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
			))) %>%
			bind_cols(i = i, after = result$data$after %||% NA, .)
		
		if (is.null(result$data$after)) {
			message('----- Break, missing AFTER')
			return(done(bind_rows(accum, parsed)))
		} else {
			return(bind_rows(accum, parsed))
		}
		
		}, .init = tibble()) %>%
		mutate(., created = with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern')) %>%
		transmute(
			.,
			method = 'top_1000_today_all', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
			)
	
	reddit$data$top_1000_today_all <<- top_1000_today_all
})

## Top (By Board) --------------------------------------------------------
local({
	
	message(str_glue('*** Pulling Top By Board: {format(now(), "%H:%M")}'))
	
	scrape_boards = collect(tbl(db, sql('SELECT * FROM sentiment_analysis_reddit_boards')))

	top_200_today_by_board = lapply(scrape_boards$board, function(board) {
		
		message('*** Pull for: ', board)
		
		# Only top possible for top
		reduce(1:2, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'day', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			# message(query)
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board, '/top?', query),
				add_headers(c(
					'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
					'Authorization' = paste0('bearer ', reddit$token)
					))
				)
			
			calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
			reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
			if (calls_remaining == 0) Sys.sleep(reset_seconds)
			result = content(http_result, 'parsed')
	
			parsed =
				lapply(result$data$children, function(y) 
					y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
				) %>%
				rbindlist(., fill = T) %>%
				select(., any_of(c(
					'name', 'subreddit', 'title', 'created',
					'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
				))) %>%
				as.data.table(.) %>%
				.[, i := i] %>%
				.[, after := result$data$after %||% NA]
			
			if (is.null(result$data$after)) {
				message('----- Break, missing AFTER')
				return(done(rbindlist(list(accum, parsed), fill = TRUE)))
			} else {
				return(rbindlist(list(accum, parsed), fill = TRUE))
			}
			
			}, .init = data.table()) %>%
			.[, created := with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern')] %>%
			return(.)
		}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'top_200_today_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
		)
	
	reddit$scrape_boards <<- scrape_boards
	reddit$data$top_200_today_by_board <<- top_200_today_by_board
})
	
## Top (By Board, Month) --------------------------------------------------------
local({
	
	message(str_glue('*** Pulling Top By Board (Old): {format(now(), "%H:%M")}'))
	
	top_1000_month_by_board = lapply(reddit$scrape_boards$board, function(board) {
		
		message('*** Pull for: ', board)
		
		# Only top possible for top
		reduce(1:9, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'month', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board, '/top?', query),
				add_headers(c(
					'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
					'Authorization' = paste0('bearer ', reddit$token)
				))
			)
			
			calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
			reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
			if (calls_remaining == 0) Sys.sleep(reset_seconds)
			result = content(http_result, 'parsed')
			
			parsed =
				lapply(result$data$children, function(y) 
					y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
				) %>%
				rbindlist(., fill = T) %>%
				select(., any_of(c(
					'name', 'subreddit', 'title', 'created',
					'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
				))) %>%
				as.data.table(.) %>%
				.[, i := i] %>%
				.[, after := result$data$after %||% NA]
			
			if (is.null(result$data$after)) {
				message('----- Break, missing AFTER')
				return(done(rbindlist(list(accum, parsed), fill = TRUE)))
			} else {
				return(rbindlist(list(accum, parsed), fill = TRUE))
			}
			
			}, .init = data.table()) %>%
			.[, created := with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern')] %>%
			return(.)
		}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'top_1000_month_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
		)
	
	# Verify no duplicated unique posts (name should be unique)
	top_1000_month_by_board %>%
		as_tibble(.) %>%
		group_by(., name) %>%
		summarize(., n = n()) %>%
		arrange(., desc(n)) %>%
		print(.)
	
	reddit$data$top_1000_month_by_board <<- top_1000_month_by_board
})

## Top (By Board, Year) --------------------------------------------------------
local({
	message(str_glue('*** Pulling Top By Board (Old): {format(now(), "%H:%M")}'))
	
	top_1000_year_by_board = lapply(reddit$scrape_boards$board, function(board) {
		
		message('*** Pull for: ', board)
		
		# Only top possible for top
		reduce(1:9, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'year', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board, '/top?', query),
				add_headers(c(
					'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
					'Authorization' = paste0('bearer ', reddit$token)
				))
			)
			
			calls_remaining = as.integer(headers(http_result)$`x-ratelimit-remaining`)
			reset_seconds = as.integer(headers(http_result)$`x-ratelimit-reset`)
			if (calls_remaining == 0) Sys.sleep(reset_seconds)
			result = content(http_result, 'parsed')
			
			parsed =
				lapply(result$data$children, function(y) 
					y[[2]] %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
				) %>%
				rbindlist(., fill = T) %>%
				select(., any_of(c(
					'name', 'subreddit', 'title', 'created',
					'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
				))) %>%
				as.data.table(.) %>%
				.[, i := i] %>%
				.[, after := result$data$after %||% NA]
			
			if (is.null(result$data$after)) {
				message('----- Break, missing AFTER')
				return(done(rbindlist(list(accum, parsed), fill = TRUE)))
			} else {
				return(rbindlist(list(accum, parsed), fill = TRUE))
			}
			
		}, .init = data.table()) %>%
			.[, created := with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern')] %>%
			return(.)
	}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'top_1000_year_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
		)
	
	# Verify no duplicated unique posts (name should be unique)
	top_1000_year_by_board %>%
		as_tibble(.) %>%
		group_by(., name) %>%
		summarize(., n = n()) %>%
		arrange(., desc(n)) %>%
		print(.)
	
	reddit$data$top_1000_year_by_board <<- top_1000_year_by_board
})


## PushShift ---------------------------------------------------------------
local({
	
	# Pull top 50 comments by day
	message(str_glue('*** Pulling Pushshift: {format(now(), "%H:%M")}'))
	
	# Get possible dates (Eastern Time)
	possible_pulls = expand_grid(
		created_dt = seq(today('US/Eastern') - days(2), as_date('2022-01-01'), '-1 day'),
		subreddit = reddit$scrape_boards$board
		)
	
	# Get existing dates (Eastern Time)
	existing_pulls = as_tibble(dbGetQuery(db, str_glue(
		"SELECT DATE(created_dttm AT TIME ZONE 'US/Eastern') AS created_dt, subreddit
		FROM sentiment_analysis_scrape_reddit
		WHERE method = 'pushshift_top_50_board_by_day'
		GROUP BY created_dt, subreddit"
		)))
	
	# Get pullable dates with UTC start and end
	new_pulls =
		anti_join(possible_pulls, existing_pulls, by = c('created_dt', 'subreddit')) %>%
		mutate(
			.,
			start = as.numeric(with_tz(force_tz(as_datetime(created_dt), 'US/Eastern'), 'UTC')),
			end = as.numeric(with_tz(force_tz(as_datetime(created_dt) + days(1), 'US/Eastern'), 'UTC')) - 1,
			) %>%
		arrange(., desc(created_dt), subreddit)

	# Now pull data
	pushshift_all_by_board =
		new_pulls %>%
		purrr::transpose(.) %>% 
		imap(., function(x, i) {
			
			message(str_glue('**** Pulling pushshift {i} of {nrow(new_pulls)}: {format(now(), "%H:%M")}'))
			
			response = GET(paste0(
					'https://api.pushshift.io/reddit/search/submission/?',
					'subreddit=', x$subreddit,
					'&size=500',
					'&sort_type=score&sort=desc',
					'&after=', x$start,
					'&before=', x$end,
					'&locked=false&stickied=false&contest_mode=false'
				))
			
			if (response$status_code == 400) return(NA)
			
			pull_names = 
				content(response)$data %>%
				map(., ~ .$id) %>%
				paste0('t3_', .)
			
			parsed =
				GET(
					paste0('https://oauth.reddit.com/api/info?id=', paste0(pull_names, collapse = ',')),
					add_headers(c(
						'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
						'Authorization' = paste0('bearer ', reddit$token)
					))) %>%
					content(.) %>%
					.$data %>%
					.$children %>%
					lapply(., function(y) 
						y$data %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
						) %>%
					rbindlist(., fill = T) %>%
					.[is.na(removed_by_category)] %>%
					select(., any_of(c(
						'name', 'subreddit', 'title', 'created',
						'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
						))) %>%
					.[, created := with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern')] %>%
					.[, scraped_dttm := now('US/Eastern')]
			
			return(parsed)
		}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'pushshift_all_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url_overridden_by_dest
		)
	
	reddit$data$pushshift_all_by_board <<- pushshift_all_by_board
})

## Store --------------------------------------------------------
local({
	
	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_scrape_reddit')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		reddit$data %>%
		bind_rows(.) %>%
		as_tibble(.) %>%
		# Format into SQL Standard style https://www.postgresql.org/docs/9.1/datatype-datetime.html
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_scrape_reddit',
				'ON CONFLICT (method, name) DO UPDATE SET
				subreddit=EXCLUDED.subreddit,
				title=EXCLUDED.title,
				created_dttm=EXCLUDED.created_dttm,
				scraped_dttm=EXCLUDED.scraped_dttm,
				selftext=EXCLUDED.selftext,
				upvote_ratio=EXCLUDED.upvote_ratio,
				ups=EXCLUDED.ups,
				is_self=EXCLUDED.is_self,
				domain=EXCLUDED.domain,
				url_overridden_by_dest=EXCLUDED.url_overridden_by_dest'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_scrape_reddit')$count)
	message('***** Rows Added: ', final_count - initial_count)
	
	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'sentiment-analysis-pull-reddit', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
	
})

# News --------------------------------------------------------

## Reset SQL --------------------------------------------------------
local({
	
	if (RESET_SQL) {
		dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_scrape_news CASCADE')
		
		dbExecute(
			db,
			'CREATE TABLE sentiment_analysis_scrape_news (
			id SERIAL PRIMARY KEY,
			source VARCHAR(255) NOT NULL,
			method VARCHAR(255) NOT NULL,
			title TEXT NOT NULL,
			created_dt DATE NOT NULL,
			description TEXT NOT NULL,
			scraped_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
			UNIQUE (source, method, title, created_dt)
			)'
		)
	}
	
})

## Pull Reuters --------------------------------------------------------
local({
	
	message(str_glue('*** Pulling Reuters Data: {format(now(), "%H:%M")}'))
	
	page_to = {if (BACKFILL_REUTERS == TRUE) 3000 else 20}
	
	reuters_data =
		reduce(1:page_to, function(accum, page) {
			
			if (page %% 20 == 1) message('Downloading data for page ', page)
			
			page_content =
				GET(paste0(
					'https://www.reuters.com/news/archive/businessnews?view=page&page=',
					 page, '&pageSize=10'
					)) %>%
				content(.) %>%
				html_node(., 'div.column1')
			
			res =
				tibble(
					page = page,
					title = html_text(html_nodes(page_content, 'h3.story-title'), trim = TRUE),
					description = html_text(html_nodes(page_content, 'div.story-content > p'), trim = TRUE),
					created = html_text(html_nodes(page_content, 'span.timestamp'), trim = TRUE)
				) %>%
				mutate(
					.,
					created = ifelse(str_detect(created, 'am |pm '), format(today(), '%b %d %Y'), created),
					created = as_date(parse_date_time2(created, '%b %d %Y'))
				) %>%
				bind_rows(accum, .)
			
			return(res)
		}, .init = tibble()) %>%
		transmute(
			.,
			source = 'reuters',
			method = 'business',
			title, created_dt = created, description, scraped_dttm = now('America/New_York')
			) %>%
		# Duplicates can be caused by shifting pages
		distinct(., title, created_dt, .keep_all = T)
	
	reuters <<- list()
	reuters$data <<- reuters_data
})


## Pull FT --------------------------------------------------------
local({

	message(str_glue('*** Pulling FT Data: {format(now(), "%H:%M")}'))

	method_map = tribble(
		~ method, ~ ft_key,
		'economics', 'ec4ffdac-4f55-4b7a-b529-7d1e3e9f150c'	
	)
	
	existing_pulls = as_tibble(dbGetQuery(db, str_glue(
		"SELECT created_dt, method
		FROM sentiment_analysis_scrape_news
		WHERE source = 'ft'
		GROUP BY created_dt, method"
		)))
	
	possible_pulls = expand_grid(
		created_dt = seq(from = as_date('2019-01-01'), to = today() - days(1), by = '1 day'),
		method = method_map$method
		)
	
	new_pulls =
		anti_join(possible_pulls, existing_pulls, by = c('created_dt', 'method')) %>%
		left_join(., method_map, by = 'method')

	ft_data =
		new_pulls %>%
		purrr::transpose(.) %>%
		imap_dfr(., function(x, i) {
			
			message(str_glue('*** Pulling data for {i} of {nrow(new_pulls)}'))
			
			url = str_glue(
				'https://www.ft.com/search?',
				'&q=%3D+NOT+010101010101',
				'&dateTo={as_date(x$created_dt)}&dateFrom={as_date(x$created_dt)}',
				'&sort=date&expandRefinements=true&contentType=article',
				'&concept={x$ft_key}'
				)
			message(url)
			
			pages =
				GET(url) %>%
				content(.) %>%
				html_node(., 'div.search-results__heading-title > h2') %>%
				html_text(.) %>%
				str_extract(., '(?<=of ).*') %>%
				as.numeric(.) %>%
				{(. - 1) %/% 25 + 1}
			
			map_dfr(1:pages, function(page) {
				search_results =
					paste0(url, '&page=',page) %>%
					GET(.) %>%
					content(.) %>%
					html_nodes(., '.search-results__list-item .o-teaser__content') %>%
					map_dfr(., function(z) tibble(
						title = z %>% html_nodes('.o-teaser__heading') %>% html_text(.),
						description = z %>% html_nodes('.o-teaser__standfirst') %>% html_text(.)
					))
				}) %>%
				mutate(., method = x$method, created_dt = as_date(x$created_dt))
			}) %>%
		transmute(
			.,
			source = 'ft',
			method,
			title, created_dt, description, scraped_dttm = now('America/New_York')
		)
	
	ft <<- list()
	ft$data <<- ft_data
})

## Store --------------------------------------------------------
local({
	
	message(str_glue('*** Sending Reuters Data to SQL: {format(now(), "%H:%M")}'))
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_scrape_news')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		bind_rows(ft$data, reuters$data) %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/2000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_scrape_news',
				'ON CONFLICT (method, title, created_dt) DO UPDATE SET
				description=EXCLUDED.description,
				scraped_dttm=EXCLUDED.scraped_dttm'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_scrape_news')$count)
	message('***** Rows Added: ', final_count - initial_count)
	
	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'sentiment-analysis-pull-news', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
})

# Finalize --------------------------------------------------------

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
