# Initialize ----------------------------------------------------------
validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)

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

	backfill_boards =
		reddit_boards %>%
		filter(., scrape_board %in% c('careerguidance', 'jobs')) %>%
		mutate(., file = file.path(fs::path_home(), paste0(scrape_board, '_submissions'))) %>%
		df_to_list()


	get_start_line = function(file, start, end, target_date = as_date('2020-01-01')) {

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


	board_res = list_rbind(map(backfill_boards, function(b) {

		lines_count = as.integer(system(str_glue('wc -l {b$file} | awk \'{{ print $1 }}\''), intern = T))

		start_line = get_start_line(b$file, 1, lines_count)
		message('Starting read @ line ', start_line, '/', lines_count)
		raw_json = read_lines(b$file, skip = start_line, progress = T)
		message('Raw JSON finished')

		raw_json %>%
			map(., .progress = T, \(x) jsonlite::fromJSON(x)) %>%
			map(., .progress = T, \(x) as_tibble(compact(keep(x, \(z) !is.list(z))))) %>%
			list_rbind(.) %>%
			select(., any_of(c(
				'id', 'name', 'subreddit', 'title', 'created_utc',
				'selftext', 'upvote_ratio', 'score', 'is_self', 'domain', 'url_overridden_by_dest'
			))) %>%
			bind_rows(
				.,
				tibble(
					id = character(), name = character(),
					upvote_ratio = double(),
					title = character(), selftext = character(),
					score = integer(), is_self = logical(), domain = character(),
					url_overridden_by_dest = character()
				)
			) %>%
			transmute(
				.,
				scrape_method = 'pushshift_backfill',
				post_id = ifelse(!is.na(name), name, ifelse(!is.na(id), id, NA)),
				scrape_board = subreddit,
				source_board = subreddit,
				title, selftext,
				upvote_ratio = replace_na(upvote_ratio, 0),
				ups = score, is_self, domain, url = url_overridden_by_dest,
				created_dttm = with_tz(as_datetime(created_utc, tz = 'UTC'), 'US/Eastern'),
				scraped_dttm = now('US/Eastern')
			) %>%
			filter(
				.,
				!is.na(post_id),
				is_self == T & !selftext %in% c('[deleted]', '[removed]', ''),
				ups >= 10
			) %>%
			# Filter out duplicated post_ids x scrape_method. This can occur rarely due to page bumping.
			slice_sample(., n = 1, by = c(post_id, scrape_method, scrape_board))
		}))

	board_res <<- board_res
})


## Store --------------------------------------------------------
local({

	message(str_glue('*** Sending Reddit Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_reddit_scrape')
	message('***** Initial Count: ', initial_count)

	insert_groups =
		board_res %>%
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

	validation_log$reddit_rows_backfilled <<- rows_added
})
