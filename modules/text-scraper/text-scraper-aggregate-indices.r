#' TBD
#'

# Initialize ----------------------------------------------------------
PROMPT_VERSION = 1

validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Get Training Data --------------------------------------------------------
input_data = get_query(pg, str_glue(
    "SELECT
        a.post_id, a.label_key, a.label_value, a.label_rationale AS rationale,
        b.title, b.selftext, b.source_board, DATE(created_dttm AT TIME ZONE 'US/Eastern') AS created_dt
    FROM text_scraper_reddit_llm_scores a
    INNER JOIN text_scraper_reddit_scrape b
        ON a.scrape_id = b.id
        AND b.source_board IN ('jobs', 'careerguidance')
    WHERE a.prompt_version = {PROMPT_VERSION}"
))


date_ranges_by_board =
	input_data %>%
	group_by(., source_board) %>%
	summarize(., min_dt = min(created_dt), max_dt = max(created_dt), .groups = 'drop')

day_by_vals_df =
	input_data %>%
	filter(., label_key == 'employment_status') %>%
	group_by(., created_dt, label_value) %>%
	summarize(., n = n(), .groups = 'drop') %>%
	right_join(
		.,
		expand_grid(
			created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day'),
			label_value = unique(.$label_value)
			),
		by = c('created_dt', 'label_value')
		) %>%
	mutate(., n = replace_na(n, 0)) %>%
	arrange(., created_dt) %>%
	mutate(., n_roll = zoo::rollsum(n, 30, fill = NA, align = 'right'), .by = 'label_value')

day_by_vals_df %>%
	ggplot() +
	geom_line(aes(x = created_dt, y = n_roll, color = label_value))

day_by_vals_df %>%
	pivot_wider(., names_from = label_value, values_from = n_roll, id_cols = created_dt) %>%
	mutate(
		.,
		emp_ratio = employed/(employed + unknown + unemployed),
		ue_ratio = unemployed/(employed + unemployed)
		) %>%
	ggplot() +
	geom_line(aes(x = created_dt, y = emp_ratio), color = 'green') +
	geom_line(aes(x = created_dt, y = ue_ratio), color = 'red')



lines_count = as.integer(system('wc -l /home/charles/jobs_submissions| awk \'{ print $1 }\'', intern = T))

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

start_line = get_start_line('/home/charles/jobs_submissions', 1, lines_count)

read_lines('/home/charles/jobs_submissions', skip = 364941 - 10000, n_max = 10000) %>%
	map(., \(x) jsonlite::fromJSON(x)) %>%
	map(., \(x) as_tibble(compact(keep(x, \(z) !is.list(z))))) %>%
	list_rbind(.) %>%
	select(., any_of(c(
		'id', 'name', 'subreddit', 'title', 'created_utc',
		'selftext', 'upvote_ratio', 'score', 'is_self', 'domain', 'url_overridden_by_dest'
	))) %>%
	bind_rows(
		.,
		tibble(
			id = character(), name = character(),
			upvote_ratio = numeric(),
			title = character(), selftext = character(),
			score = integer(), is_self = logical(), domain = character(),
			url_overridden_by_dest = character()
			)
		) %>%
	transmute(
		.,
		scrape_method = 'pushshift_backfill',
		post_id = ifelse(!is.na(name), name, ifelse(!is.na(id), id, NA)),
		scrape_board = 'jobs',
		source_board = subreddit,
		title, selftext, upvote_ratio, ups = score, is_self, domain, url = url_overridden_by_dest,
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
