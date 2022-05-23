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
		
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_reddit_scrape CASCADE')

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_reddit_scrape (
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
		url TEXT,
		UNIQUE (method, name)
		)'
	)

}
})

## Boards --------------------------------------------------------
local({
	
	scrape_boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE scrape_active = TRUE"
		)))

	reddit <<- list()
	reddit$scrape_boards <<- scrape_boards
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
			selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest
			)
	
	reddit$data$top_1000_today_all <<- top_1000_today_all
})

## Top (By Board) --------------------------------------------------------
local({

	message(str_glue('*** Pulling Top 200/Day By Board: {format(now(), "%H:%M")}'))
	
	top_200_today_by_board = lapply(purrr::transpose(reddit$scrape_boards), function(board) {
		
		message('*** Pull for: ', board$subreddit)
		
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
				paste0('https://oauth.reddit.com/r/', board$subreddit, '/top?', query),
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
				.[, after := result$data$after %||% NA] %>%
				.[ups >= board$scrape_ups_floor]
			
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
			selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest
		)
	
	reddit$data$top_200_today_by_board <<- top_200_today_by_board
})

## Top (By Board, Week) --------------------------------------------------------
local({
	
	message(str_glue('*** Pulling Top 1k/Week By Board: {format(now(), "%H:%M")}'))
	
	top_1000_week_by_board = lapply(purrr::transpose(reddit$scrape_boards), function(board) {
		
		message('*** Pull for: ', board$subreddit)
		
		# Only top possible for top
		reduce(1:9, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'week', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board$subreddit, '/top?', query),
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
				.[, after := result$data$after %||% NA] %>%
				.[ups >= board$scrape_ups_floor]
			
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
			method = 'top_1000_week_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest
		)
	
	# Verify no duplicated unique posts (name should be unique)
	top_1000_week_by_board %>%
		as_tibble(.) %>%
		group_by(., name) %>%
		summarize(., n = n()) %>%
		arrange(., desc(n)) %>%
		print(.)
	
	reddit$data$top_1000_week_by_board <<- top_1000_week_by_board
})

## Top (By Board, Month) --------------------------------------------------------
local({
	
	message(str_glue('*** Pulling Top 1k/Month By Board: {format(now(), "%H:%M")}'))
	
	top_1000_month_by_board = lapply(purrr::transpose(reddit$scrape_boards), function(board) {
		
		message('*** Pull for: ', board$subreddit)
		
		# Only top possible for top
		reduce(1:9, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'month', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board$subreddit, '/top?', query),
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
				.[, after := result$data$after %||% NA] %>%
				.[ups >= board$scrape_ups_floor]
			
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
			selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest
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
	message(str_glue('*** Pulling Top 1k/Year By Board: {format(now(), "%H:%M")}'))
	
	top_1000_year_by_board = lapply(purrr::transpose(reddit$scrape_boards), function(board) {
		
		message('*** Pull for: ', board$subreddit)
		
		# Only top possible for top
		reduce(1:9, function(accum, i) {
			
			message('***** Pull ', i)
			query =
				list(t = 'year', limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
				compact(.) %>%
				paste0(names(.), '=', .) %>%
				paste0(collapse = '&')
			
			http_result = GET(
				paste0('https://oauth.reddit.com/r/', board$subreddit, '/top?', query),
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
				.[, after := result$data$after %||% NA] %>%
				.[ups >= board$scrape_ups_floor]
			
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
			selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest
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
	
	# List of subreddits to force a full repull of data
	# Leave as an empty vector generally
	BOARDS_FULL_REPULL = c()

	# Pull top 50 comments by day
	message(str_glue('*** Pulling Pushshift: {format(now(), "%H:%M")}'))
	
	# Get possible dates (Eastern Time)
	possible_pulls = expand_grid(
		# Pushshift can have a delay up to 3 days
		# Start from 2019-01-01 if no data, but can be skipped to later if older data already pulled
		# As of 5/23/22: Moved back to 4/1/2018
		created_dt = seq(today('US/Eastern') - days(4), as_date('2019-01-01'), '-1 day'),
		reddit$scrape_boards
		)
	
	# Get existing dates (Eastern Time)
	existing_pulls = as_tibble(dbGetQuery(db, str_glue(
		"SELECT DATE(created_dttm AT TIME ZONE 'US/Eastern') AS created_dt, subreddit, COUNT(*) as count_existing
		FROM sentiment_analysis_reddit_scrape
		WHERE method = 'pushshift_all_by_board'
		GROUP BY created_dt, subreddit"
		)))
	# Append to re-pull (perhaps for 5/4/22 fix?)
	#%>%
		#filter(., 1 == 0)

	# Get pullable dates with UTC start and end
	new_pulls =
		anti_join(
			possible_pulls,
			# Things to not pull
			existing_pulls %>%
				# 5/20/22: Forcibly repull jobs & careerguidance
				filter(., !subreddit %in% BOARDS_FULL_REPULL) %>% 
				# Always repull last week
				filter(., created_dt <= today() - days(7)),
			by = c('created_dt', 'subreddit')
			) %>%
		mutate(
			.,
			start = format(
				as.numeric(with_tz(force_tz(as_datetime(created_dt), 'US/Eastern'), 'UTC')),
				scientific = F
				),
			end = format(
				as.numeric(with_tz(force_tz(as_datetime(created_dt) + days(1), 'US/Eastern'), 'UTC')) - 1,
				scientific = F
				),
			) %>%
		arrange(., desc(created_dt), subreddit)

	message('***** New Pulls:')
	print(new_pulls, n = 200)

	# Now pull data
	pushshift_all_by_board =
		new_pulls %>%
		purrr::transpose(.) %>%
		imap(., function(x, i) {
			
			message(str_glue(
				'-----\n***** Pulling pushshift {i} of {nrow(new_pulls)}: {format(now(), "%H:%M")} ',
				'| Date: {as_date(x$created_dt)} | Board: {x$subreddit}'
				))
			
			# Now pull all submissions for that date and subreddit
			scrape_names_raw = local({
				last_page = F
				start = x$start
				page = 1
				all_ids = na.omit(tibble(pull_names = NA_character_, page = NA_integer_))
				while(last_page == F) {
					url = paste0(
						'https://api.pushshift.io/reddit/search/submission/?',
						'subreddit=', x$subreddit,
						'&size=100&limit=100',
						'&sort_type=created_utc&sort=asc',
						'&after=', start,
						'&before=', x$end,
						'&locked=false&stickied=false&contest_mode=false'
					)
					message(page, ' ', url)
					response = content(RETRY('GET', url, times = 20))$data
					if (length(response) == 0) {
						message('***** End | Empty Response | Page: ', page)
						break
					}
					pull_names = response %>% map(., ~ .$id) %>% paste0('t3_', .)
					created_dts = response %>% map(., ~ .$created)
					all_ids = bind_rows(all_ids, tibble(pull_names = pull_names, page = page))
					# Also will end on returning <100 results - Bug found on 5/4/22 where some 98/99 result returns 
					# wouldn't be actual end
					# if (length(pull_names) < 100 || response[[100]]$created > x$end) {
					# 	message('****** End | Length: ' , length(response), ' | Page: ', page, ' | Board: ', x$subreddit)
					# 	last_page = T
					# }	else {
					# 	start = created_dts[[100]]
					# 	last_page = F
					# 	page = page + 1
					# }
					# Pages can return 98/99 length even if there are more pages available
					if (length(response) < 90 || response[[length(response)]]$created > x$end) {
						message('****** End | Length: ', length(response), ' | Page: ', page)
						last_page = T
					}	else {
						start = created_dts[[length(created_dts)]]
						last_page = F
						page = page + 1
					}
					Sys.sleep(.1)
				}
				return(all_ids)
			})
			
			# If no results existed at all for this subreddit/date combination
			if (nrow(scrape_names_raw) == 0) {
				message('***** WARNING: No rows returned, skipping: \n')
				return(tibble())
			}
			message('****** Pushshift API pulled: ', length(unique(scrape_names_raw$pull_names)), ' rows')
			
			pulled_data =
				scrape_names_raw %>%
				distinct(., pull_names) %>%
				mutate(., group = (1:nrow(.)) %/% 50) %>%
				group_split(., group) %>%
				lapply(., function(split_group) {
					parsed =
						RETRY(
							'GET',
							url = paste0('https://oauth.reddit.com/api/info?id=', paste0(split_group$pull_names, collapse = ',')),
							times = 20,
							add_headers(c(
								'User-Agent' = 'windows:SentimentAnalysis:v0.0.1 (by /u/dongobread)',
								'Authorization' = paste0('bearer ', reddit$token),
								'Content-Type' = 'application/json'
							))) %>%
							content(.) %>%
							.$data %>%
							.$children %>%
							lapply(., function(y) 
								y$data %>% keep(., ~ !is.null(.) && !is.list(.)) %>% as_tibble(.)
								) %>%
							rbindlist(., fill = T) %>%
							select(., any_of(c(
								'name', 'subreddit', 'title', 'created',
								'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest',
								'removed_by_category'
								)))
					return(parsed)
				}) %>%
				rbindlist(., fill = TRUE)
			
			message('****** Reddit API pulled (pre-filtration): ', nrow(pulled_data), ' rows')
			
			pulled_data_cleaned = 
				pulled_data %>%
				{
					if ('removed_by_category' %in% colnames(.)) .[is.na(removed_by_category)] 
					else .
				} %>%
				.[title != '[deleted by user]' & selftext != '[removed]'] %>%
				.[ups >= x$scrape_ups_floor] %>%
				.[, created := with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern')] %>%
				.[, scraped_dttm := now('US/Eastern')]

			
			message('****** Reddit API pulled (post-filtration): ', nrow(pulled_data_cleaned), ' rows')
			
			return(pulled_data_cleaned)
		}) %>%
		rbindlist(., fill = TRUE) %>%
		transmute(
			.,
			method = 'pushshift_all_by_board', name,
			subreddit, title, 
			created_dttm = created, scraped_dttm = now('US/Eastern'),
			selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest
			)
	
	reddit$data$pushshift_all_by_board <<- pushshift_all_by_board
})

## Store --------------------------------------------------------
local({
	
	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_reddit_scrape')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		reddit$data %>%
		rbindlist(.) %>%
		.[!is.na(name) & !is.na(subreddit) & !is.na(title)] %>%
		# Format into SQL Standard style https://www.postgresql.org/docs/9.1/datatype-datetime.html
		.[,
			c('created_dttm', 'scraped_dttm') := lapply(.SD, function(x) format(x, '%Y-%m-%d %H:%M:%S %Z')),
			.SDcols = c('created_dttm', 'scraped_dttm')
			] %>%
		mutate(., across(where(is.character), function(x) ifelse(str_length(x) == 0, NA, x))) %>%
		mutate(., split = ceiling((1:nrow(.))/10000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_reddit_scrape',
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
				url=EXCLUDED.url'
				) %>%
				 dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_reddit_scrape')$count)
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
		dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_media_scrape CASCADE')
		
		dbExecute(
			db,
			'CREATE TABLE sentiment_analysis_media_scrape (
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
	
	page_to = 20 #100 normally, 3000 for backfill
	
	reuters_data =
		reduce(1:page_to, function(accum, page) {
			
			if (page %% 20 == 1) message('***** Downloading data for page ', page)
			
			page_content =
				RETRY('GET', paste0(
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
			title, created_dt = created, description, scraped_dttm = now('US/Eastern')
			) %>%
		# Duplicates can be caused by shifting pages
		distinct(., title, created_dt, .keep_all = T)
	
	media <<- list()
	media$data$reuters <<- reuters_data
})


## Pull FT --------------------------------------------------------
local({

	# Note 4/17/22: To test this, must 
	message(str_glue('*** Pulling FT Data: {format(now(), "%H:%M")}'))

	method_map = tribble(
		~ method, ~ ft_key,
		'economics', 'ec4ffdac-4f55-4b7a-b529-7d1e3e9f150c'	
	)
	
	existing_pulls = as_tibble(dbGetQuery(db, str_glue(
		"SELECT created_dt, method, COUNT(*) as count
		FROM sentiment_analysis_media_scrape
		WHERE source = 'ft'
		GROUP BY created_dt, method"
		)))
	
	possible_pulls = expand_grid(
		created_dt = seq(from = as_date('2020-01-01'), to = today() + days(1), by = '1 day'),
		method = method_map$method
		)
	
	new_pulls =
		anti_join(
			possible_pulls,
			# Always pull last week articles
			existing_pulls %>% filter(., created_dt <= today() - days(7)),
			by = c('created_dt', 'method')
			) %>%
		left_join(., method_map, by = 'method')
	
	message('*** New Pulls')
	print(new_pulls)

	ft_data =
		new_pulls %>%
		purrr::transpose(.) %>%
		imap(., function(x, i) {
			
			message(str_glue('***** Pulling data for {i} of {nrow(new_pulls)}'))
			url = str_glue(
				'https://www.ft.com/search?',
				'&q=-010101010101',
				'&dateFrom={as_date(x$created_dt)}&dateTo={as_date(x$created_dt) + days(1)}',
				'&sort=date&expandRefinements=true&contentType=article',
				'&concept={x$ft_key}'
				)
			message(url)
			
			page1 =	
				RETRY(
					'GET',
					url,
					add_headers(c(
						'User-Agent' = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:99.0) Gecko/20100101 Firefox/99.0'
						))
					) %>%
				content(.)
	
			pages =
				page1 %>%
				html_node(., 'div.search-results__heading-title > h2') %>%
				html_text(.) %>%
				str_replace_all(., coll('Powered By Algolia'), '') %>%
				str_extract(., '(?<=of ).*') %>%
				as.numeric(.) %>%
				{(. - 1) %/% 25 + 1}
			
			message('get_dta')
			if (is.na(pages)) return(NULL)
			
			map_dfr(1:pages, function(page) {
				
				this_page = {if(page == 1) page1 else content(RETRY('GET', paste0(url, '&page=',page)))}
				search_results =
					this_page %>%
					html_nodes(., '.search-results__list-item .o-teaser__content') %>%
					map_dfr(., function(z) tibble(
						title = z %>% html_nodes('.o-teaser__heading') %>% html_text(.),
						description = z %>% html_nodes('.o-teaser__standfirst') %>% html_text(.)
					))
				}) %>%
				mutate(., method = x$method, created_dt = as_date(x$created_dt))
			}) %>%
		keep(., ~ !is.null(.) & is_tibble(.)) %>%
		{
			if (length(.) >= 1)
			bind_rows(.) %>%
				transmute(
					.,
					source = 'ft',
					method,
					title, created_dt, description, scraped_dttm = now('US/Eastern')
				)
			else tibble()
		}
	
	media$data$ft <<- ft_data
})

## Store --------------------------------------------------------
local({
	
	message(str_glue('*** Sending Media Data to SQL: {format(now(), "%H:%M")}'))
	
	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_media_scrape')$count)
	message('***** Initial Count: ', initial_count)
	
	sql_result =
		bind_rows(media$data) %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/2000)) %>%
		group_by(., source, method, title, created_dt) %>%
		slice_head(., n = 1) %>%
		ungroup(.) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_media_scrape',
				'ON CONFLICT (source, method, title, created_dt) DO UPDATE SET
				description=EXCLUDED.description,
				scraped_dttm=EXCLUDED.scraped_dttm'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}
	
	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_media_scrape')$count)
	message('***** Rows Added: ', final_count - initial_count)
	
	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'sentiment-analysis-pull-media', today(), 'job-success',
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
