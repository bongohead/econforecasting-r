# Initialize ----------------------------------------------------------
validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(data.table)

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Backfill ----------------------------------------------------------------

## Get Desired Boards ------------------------------------------------------
reddit_boards = get_query(pg, sql(
	"SELECT scrape_board, scrape_ups_floor
	FROM text_scraper_reddit_boards
	WHERE scrape_active = true"
))

## Backfill ------------------------------------------------------
local({

	# reddit-top20k.cworld.ai
	#  wget https://reddit-archive.cworld.ai/careerguidance_submissions.zst && zstd --decompress careerguidance_submissions
	get_start_line = function(file, start, end, target_date = as_date('2019-01-01')) {
		
		partition_at = floor((end + start)/2)
		
		line = read_lines(file, skip = partition_at, n_max = 1)
		line_date = as_date(as_datetime(jsonlite::fromJSON(line)$created_utc, tz = 'UTC'))
		
		if (line_date < target_date) {
			get_start_line(file = file, start = partition_at, end = end, target_date = target_date)
		} else if (line_date > target_date) {
			get_start_line(file = file, start = start, end = partition_at, target_date = target_date)
		} else {
			return(partition_at)
		}
	}
	
	backfill_boards =
		reddit_boards %>%
		filter(., scrape_board %in% c('personalfinance')) %>%
		mutate(., file = file.path(fs::path_home(), paste0(scrape_board, '_submissions'))) %>%
		rowwise(.) %>%
		mutate(
			., 
			lines_count = as.integer(system(str_glue('wc -l {file} | awk \'{{ print $1 }}\''), intern = T)),
			start_line = get_start_line(file, 1, lines_count),
			total_lines = lines_count - start_line
			) %>%
		ungroup(.)
	
	# Split into groups due to memory constraints
	scrape_groups =
		backfill_boards %>%
		group_split(., scrape_board, scrape_ups_floor, file) %>%
		map(., \(x)
			tibble(line = b$start_line:b$lines_count) %>%
				mutate(., line_group = ceiling(line/50000)) %>%
				group_by(., line_group) %>% 
				summarize(., start = min(line), end = max(line), .groups = 'drop') %>%
				mutate(., lines_to_read = end - start + 1) %>%
				bind_cols(select(x, scrape_board, scrape_ups_floor, file), .)
		) %>%
		list_rbind()
		

	board_res = list_rbind(map(df_to_list(scrape_groups), .progress = T, function(b) {
		
		raw_json = read_lines(b$file, skip = b$start, progress = T, n_max = b$lines_to_read)
		
		parsed_json =
			RcppSimdJson::fparse(raw_json, max_simplify_lvl = 'list') %>%
			map(., \(x) compact(keep(x, \(z) !is.list(z)))) %>%
			map(., as.data.table) %>%
			rbindlist(., fill = T) %>%
			select(., any_of(c(
				'id', 'name', 'subreddit', 'title', 'created_utc',
				'selftext', 'upvote_ratio', 'score', 'is_self', 'domain', 'url_overridden_by_dest'
			))) %>%
			list(
				.,
				data.table(
					id = character(), name = character(),
					upvote_ratio = double(),
					title = character(), selftext = character(),
					score = integer(), is_self = logical(), domain = character(),
					url_overridden_by_dest = character()
				)
			) %>%
			rbindlist(., fill = T) %>%
			.[, scrape_method:= 'pushshift_backfill'] %>%
			.[, post_id := fifelse(!is.na(name), name, fifelse(!is.na(id), id, NA_character_))] %>%
			.[, scrape_board := subreddit] %>%
			.[, source_board := subreddit] %>%
			.[, ups := score] %>%
			.[, url := url_overridden_by_dest] %>%
			.[, created_dttm := with_tz(as_datetime(created_utc, tz = 'UTC'), 'US/Eastern')] %>%
			.[, scraped_dttm := now('US/Eastern')]  %>%
			.[
				!is.na(post_id) &
				is_self == T & !selftext %in% c('[deleted]', '[removed]', '') &
				ups >= b$scrape_ups_floor
			] %>%
			# Filter out duplicated post_ids x scrape_method. This can occur rarely due to page bumping.
			slice_sample(., n = 1, by = c(post_id, scrape_method, scrape_board)) %>%
			select(
				.,
				scrape_method,
				post_id,
				scrape_board,
				source_board,
				title, selftext,
				upvote_ratio,
				ups, is_self, domain, url,
				created_dttm,
				scraped_dttm
			)
		
		return(parsed_json)

		# Pure dplyr version
		# raw_json %>%
		# 	map(., .progress = T, \(x) as_tibble(compact(keep(x, \(z) !is.list(z))))) %>%
		# 	list_rbind(.) %>%
		# 	select(., any_of(c(
		# 		'id', 'name', 'subreddit', 'title', 'created_utc',
		# 		'selftext', 'upvote_ratio', 'score', 'is_self', 'domain', 'url_overridden_by_dest'
		# 	))) %>%
		# 	bind_rows(
		# 		.,
		# 		tibble(
		# 			id = character(), name = character(),
		# 			upvote_ratio = double(),
		# 			title = character(), selftext = character(),
		# 			score = integer(), is_self = logical(), domain = character(),
		# 			url_overridden_by_dest = character()
		# 		)
		# 	) %>%
		# 	transmute(
		# 		.,
		# 		scrape_method = 'pushshift_backfill',
		# 		post_id = ifelse(!is.na(name), name, ifelse(!is.na(id), id, NA)),
		# 		scrape_board = subreddit,
		# 		source_board = subreddit,
		# 		title, selftext,
		# 		upvote_ratio,
		# 		ups = score, is_self, domain, url = url_overridden_by_dest,
		# 		created_dttm = with_tz(as_datetime(created_utc, tz = 'UTC'), 'US/Eastern'),
		# 		scraped_dttm = now('US/Eastern')
		# 	) %>%
		# 	filter(
		# 		.,
		# 		!is.na(post_id),
		# 		is_self == T & !selftext %in% c('[deleted]', '[removed]', ''),
		# 		ups >= 10
		# 	) %>%
		# 	# Filter out duplicated post_ids x scrape_method. This can occur rarely due to page bumping.
		# 	slice_sample(., n = 1, by = c(post_id, scrape_method, scrape_board))
		}))

	board_res <<- board_res
})


## Store --------------------------------------------------------
local({

	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_reddit_scrapes')
	message('***** Initial Count: ', initial_count)

	insert_groups =
		board_res %>%
		# Format into SQL Standard style https://www.postgresql.org/docs/9.1/datatype-datetime.html
		mutate(
			.,
			across(c(created_dttm, scraped_dttm), \(x) format(x, '%Y-%m-%d %H:%M:%S %Z')),
			split = ceiling((1:nrow(.))/10000)
		) %>%
		group_split(., split, .keep = F)

	insert_result = map_dbl(insert_groups, .progress = F, function(x)
		dbExecute(pg, create_insert_query(
			x,
			'text_scraper_reddit_scrapes',
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

	final_count = get_rowcount(pg, 'text_scraper_reddit_scrapes')
	rows_added = final_count - initial_count

	message('***** Rows Added: ', rows_added)

	validation_log$reddit_rows_backfilled <<- rows_added
})
