#' Scrapes text from different sources

# Initialize ----------------------------------------------------------
validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)
library(RCurl, include.only = 'base64')
library(DBI, include.only = 'dbExecute')

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

scrape_data <- list()

# Data 1 --------------------------------------------------------

## Reddit --------------------------------------------------------
local({

	message(str_glue('*** Pulling Reddit Data: {format(now(), "%H:%M")}'))

	reddit_boards = get_query(
		pg,
		"SELECT scrape_board, scrape_ups_floor
		FROM text_scraper_reddit_boards
		WHERE scrape_active = true"
		)

	reddit_token =
		request('https://www.reddit.com/api/v1/access_token') %>%
		req_headers(
			'User-Agent' = reddit_ua,
			'Authorization' = paste0(
				'Basic ',
				base64(
					txt = paste0(Sys.getenv('REDDIT_ID'), ':', Sys.getenv('REDDIT_SECRET')),
					mode = 'character'
				)
			)
		) %>%
		req_body_form(
			grant_type = 'client_credentials',
			username = Sys.getenv('REDDIT_USERNAME'),
			password = Sys.getenv('REDDIT_PASSWORD')
		) %>%
		req_perform() %>%
		resp_body_json() %>%
		.$access_token

	# Run monthly year and all
	search_combinations = expand_grid(
		reddit_boards,
		tribble(
			~freq, ~ scrape_method, ~ max_pages,
			'day', 'top_1000_today', 9,
			'week', 'top_1000_week', 9,
			'month', 'top_1000_month', 9,
			'year', 'top_1000_year', 9
			)
		) %>%
		# Only get top monthly on Sundays, top annually on the first day of month
		{if(wday(today('US/Eastern')) == 1) . else filter(., freq != 'month')} %>%
		{if(day(today('US/Eastern')) == 1) . else filter(., freq != 'year')}

	scrape_results = map(df_to_list(search_combinations), .progress = T, function(s) {
		get_reddit_data_by_board(
		 	s$scrape_board,
		 	freq = s$freq,
		 	ua = Sys.getenv('REDDIT_UA'),
		 	token = reddit_token,
		 	max_pages = s$max_pages,
		 	min_upvotes = s$scrape_ups_floor ,
		 	.verbose = F
		 	) %>%
			mutate(., scrape_method = s$scrape_method, scrape_board = s$scrape_board)
	})

	cleaned_results =
		scrape_results %>%
		list_rbind() %>%
		transmute(
			.,
			scrape_method,
			post_id = name,
			scrape_board,
			source_board = subreddit,
			title, selftext, upvote_ratio, ups, is_self, domain, url = url_overridden_by_dest,
			created_dttm = with_tz(as_datetime(created, tz = 'UTC'), 'US/Eastern'),
			scraped_dttm = now('US/Eastern')
		) %>%
		# Filter out duplicated post_ids x scrape_method. This can occur rarely due to page bumping.
		slice_sample(., n = 1, by = c(post_id, scrape_method, scrape_board))


	# Check dupes
	cleaned_results %>%
		group_by(., post_id, scrape_method) %>%
		summarize(., n = n(), boards = paste0(scrape_board, collapse = ', '), .groups = 'drop') %>%
		arrange(., desc(n)) %>%
		filter(., n > 1) %>%
		print(.)


	scrape_data$reddit <<- cleaned_results
})


## Store --------------------------------------------------------
local({

	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_reddit_scrape')
	message('***** Initial Count: ', initial_count)

	insert_groups =
		bind_rows(scrape_data$reddit) %>%
		filter(., !is.na(post_id) & !is.na(title) & !is.na(scrape_board) & !is.na(source_board)) %>%
		# Format into SQL Standard style https://www.postgresql.org/docs/9.1/datatype-datetime.html
		mutate(
			.,
			across(c(created_dttm, scraped_dttm), \(x) format(x, '%Y-%m-%d %H:%M:%S %Z')),
			across(where(is.character), function(x) ifelse(str_length(x) == 0, NA, x)),
			split = ceiling((1:nrow(.))/10000)
			) %>%
		group_split(., split, .keep = F)

	insert_result = map_dbl(insert_groups, .progress = F, function(x)
		dbExecute(pg, create_insert_query(
			x,
			'text_scraper_reddit_scrape',
			'ON CONFLICT (scrape_method, post_id, scrape_board) DO UPDATE SET
				source_board=EXCLUDED.source_board,
				title=EXCLUDED.title,
				selftext=EXCLUDED.selftext,
				upvote_ratio=EXCLUDED.upvote_ratio,
				ups=EXCLUDED.ups,
				is_self=EXCLUDED.is_self,
				domain=EXCLUDED.domain,
				url=EXCLUDED.url,
				created_dttm=EXCLUDED.created_dttm,
				scraped_dttm=EXCLUDED.scraped_dttm'
		))
	)

	insert_result = {if (any(is.null(insert_result))) stop('SQL Error!') else sum(insert_result)}

	final_count = get_rowcount(pg, 'text_scraper_reddit_scrape')
	rows_added = final_count - initial_count


	message('***** Rows Added: ', rows_added)

	validation_log$reddit_rows_added <<- rows_added
})


# Data 2 --------------------------------------------------------

## Pull Reuters --------------------------------------------------------
local({

	message(str_glue('*** Pulling Reuters Data: {format(now(), "%H:%M")}'))

	pages = 1:3000 #100 normally, 3000 for backfill

	http_responses = send_async_requests(
		map(paste0(
			'https://www.reuters.com/news/archive/businessnews?view=page&page=',
			pages, '&pageSize=10'
		), \(x) request(x)),
		.chunk_size = 20,
		.max_retries = 5,
		.verbose = T
		)

	cleaned_responses = map(http_responses, function(x) {

		resp_body_html(x) %>%
			html_node(., 'div.column1')
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
		)
	})


	reuters_data =
		reduce(1:page_to, function(accum, page) {

			if (page %% 10 == 1) message('***** Downloading data for page ', page)

			page_content =
				request(paste0(
					'https://www.reuters.com/news/archive/businessnews?view=page&page=',
					page, '&pageSize=10'
					)) %>%
				req_retry() %>%
				req_perform() %>%
				resp_body_html() %>%
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


