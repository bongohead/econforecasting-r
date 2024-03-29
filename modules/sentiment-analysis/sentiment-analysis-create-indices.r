#' Sentiment Analysis with Dictionary Analysis & BERT
#' Run this script after score-data finishes (will take approx. 2 hours)

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-create-indices'
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
library(DBI)
library(RPostgres)
library(lubridate)
library(jsonlite)
library(highcharter)

## Load Connection Info ----------------------------------------------------------
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
run_id = log_start_in_db(db, JOB_NAME, 'sentiment-analysis')

# Data Prep --------------------------------------------------------

## Create Tables --------------------------------------------------------
local({
if (RESET_SQL) {

	# dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_indices CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_index_values CASCADE')
	# dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_benchmarks CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_benchmark_values CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_index_roberta_values CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_subreddit_roberta_values CASCADE')

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_indices (
			id SERIAL PRIMARY KEY,
			content_type VARCHAR(255) NOT NULL,
			source_type VARCHAR(255) NOT NULL,
			source_type_methods VARCHAR(255) [],
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
		)'
	)

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_index_values (
			index_id INTEGER NOT NULL,
			date DATE NOT NULL,
			count INTEGER NOT NULL,
			score DECIMAL(10, 4) NOT NULL,
			score_7dma DECIMAL(10, 4) NOT NULL,
			score_14dma DECIMAL(10, 4) NOT NULL,
			score_adj DECIMAL(10, 4) NOT NULL,
			score_adj_7dma DECIMAL(10, 4) NOT NULL,
			score_adj_14dma DECIMAL(10, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (index_id, date),
			CONSTRAINT sentiment_analysis_index_values_fk FOREIGN KEY (index_id)
				REFERENCES sentiment_analysis_indices (id) ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_benchmarks (
			varname VARCHAR(255) PRIMARY KEY,
			fullname VARCHAR(255) NOT NULL,
			pull_source VARCHAR(255) NOT NULL,
			source_key VARCHAR(255) NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
		)'
	)

	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_benchmark_values (
			varname VARCHAR(255) NOT NULL,
			date DATE NOT NULL,
			value DECIMAL(10, 2) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (varname, date),
			CONSTRAINT sentiment_analysis_benchmark_values_fk FOREIGN KEY (varname)
				REFERENCES sentiment_analysis_benchmarks (varname) ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)


	dbExecute(
		db,
		"CREATE TABLE sentiment_analysis_index_roberta_values (
			index_id INTEGER NOT NULL,
			date DATE NOT NULL,
			emotion VARCHAR(100) NOT NULL,
			value DECIMAL(10, 2) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (index_id, date, emotion)
		)"
	)

	dbExecute(
		db,
		"CREATE INDEX sentiment_analysis_index_roberta_values_subreddit_idx
			ON sentiment_analysis_index_roberta_values USING btree (index_id)"
	)

	dbExecute(
		db,
		"CREATE TABLE sentiment_analysis_subreddit_roberta_values (
			subreddit VARCHAR(255) NOT NULL,
			date DATE NOT NULL,
			emotion VARCHAR(100) NOT NULL,
			value DECIMAL(10, 2) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (subreddit, date, emotion)
		)"
	)

	dbExecute(
		db,
		"CREATE INDEX sentiment_analysis_roberta_values_subreddit_idx
			ON sentiment_analysis_subreddit_roberta_values USING btree (subreddit)"
		)

}
})

# Reddit ------------------------------------------------------------------

## Pull Data ------------------------------------------------------------------
local({

	boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE score_active = TRUE
		AND category IN ('financial', 'news', 'labor_market')"
	)))

	pushshift_data = dbGetQuery(db, str_glue(
		"SELECT
			-- Timezone is critical or will default to the UTC Date
			r1.method AS source, r1.subreddit, DATE(r1.created_dttm AT TIME ZONE 'US/Eastern') AS created_dt, r1.ups,
			r2.score_model, r2.score, r2.score_conf, r2.scored_dttm,
			r1.name, b.category AS content_type
		FROM sentiment_analysis_reddit_scrape r1
		INNER JOIN sentiment_analysis_reddit_score r2
			ON r1.id = r2.scrape_id
		INNER JOIN sentiment_analysis_reddit_boards b
			ON r1.subreddit = b.subreddit
		WHERE r1.method = 'pushshift_all_by_board'
			AND text_part = 'all_text'
			AND score_model IN ('DISTILBERT', 'ROBERTA', 'DICT')
			AND DATE(created_dttm) >= '2019-01-01'
			AND r1.ups >= b.score_ups_floor"
		)) %>%
		as_tibble(.) %>%
		arrange(., created_dt)

	recent_data = dbGetQuery(db, str_glue(
		"WITH cte AS
		(
			SELECT
				r1.method AS source, r1.subreddit, DATE(r1.created_dttm AT TIME ZONE 'US/Eastern') AS created_dt, r1.ups,
				r2.score_model, r2.score, r2.score_conf, r2.scored_dttm,
				b.category AS content_type,
				-- Get latest scraped value of post if multiple
				r1.name, r1.scraped_dttm,
				ROW_NUMBER() OVER (PARTITION BY r1.name, r2.score_model ORDER BY scraped_dttm DESC) AS rn
			FROM sentiment_analysis_reddit_scrape r1
			INNER JOIN sentiment_analysis_reddit_score r2
				ON r1.id = r2.scrape_id
			INNER JOIN sentiment_analysis_reddit_boards b
				ON r1.subreddit = b.subreddit
			WHERE r1.method IN ('top_200_today_by_board', 'top_1000_week_by_board', 'top_1000_month_by_board')
				AND text_part = 'all_text'
				AND score_model IN ('DISTILBERT', 'ROBERTA', 'DICT')
				AND DATE(created_dttm) >= '2019-01-01'
				AND r1.ups >= b.score_ups_floor
		)
		SELECT * FROM cte WHERE rn = 1"
		)) %>%
		as_tibble(.) %>%
		arrange(., created_dt)

	# # Get list of score_model x subreddit x created_dt combinations not already in pushshift_data
	# to_keep =
	# 	recent_data %>%
	# 	group_by(., score_model, subreddit, created_dt) %>%
	# 	summarize(., .groups = 'drop') %>%
	# 	anti_join(
	# 		.,
	# 		pushshift_data %>%
	# 			group_by(., score_model, subreddit, created_dt) %>%
	# 			summarize(., .groups = 'drop'),
	# 		by = c('score_model', 'subreddit', 'created_dt')
	# 	)
	#
	# kept_recent_data =
	# 	recent_data %>%
	# 	inner_join(., to_keep, by = c('score_model', 'subreddit', 'created_dt'))
	#
	# data = bind_rows(
	# 	pushshift_data %>% mutate(., finality = 'pushshift'),
	# 	kept_recent_data %>% mutate(., finality = 'top_200')
	# 	)

	# 5/18/22 - Use all available recent_data, just dump anything that overlaps with Pushshift data
	# Overlap verification
	recent_data %>%
		mutate(., in_pushshift = name %in% pushshift_data$name) %>%
		group_by(., created_dt, in_pushshift) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(
			.,
			id_cols = 'created_dt', values_from = n,
			names_from = in_pushshift, names_prefix = 'in_pushshift_'
			) %>%
		arrange(., desc(created_dt)) %>%
		print(., n = 20)

	kept_recent_data =
		recent_data %>%
		filter(., !name %in% pushshift_data$name)

	data = bind_rows(
		pushshift_data %>% mutate(., finality = 'pushshift'),
		kept_recent_data %>% mutate(., finality = 'top_200')
		)

	# Prelim Checks
	count_by_source_plot =
		data %>%
		filter(., created_dt >= today() - months(3)) %>%
		group_by(., created_dt, source) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		arrange(., created_dt) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = n, color = source))

	count_by_model_plot =
		data %>%
		group_by(., created_dt, score_model, finality) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		arrange(., created_dt) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = n, color = score_model, linetype = finality))

	count_by_board_plot =
		data %>%
		group_by(., created_dt, subreddit) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		arrange(., created_dt) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = n, color = subreddit))

	# Scores by group
	distilbert_score_by_subreddit =
		data %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		group_by(., created_dt, subreddit, score_model, finality) %>%
		summarize(., mean_score = mean(score), .groups = 'drop') %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score, color = score_model, linetype = finality)) +
		facet_wrap(vars(subreddit))

	# Merge Coutns
	distilbert_merge_counts =
		data %>%
		filter(., score_model == 'DISTILBERT') %>%
		group_by(., finality, created_dt) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(., names_from = finality, values_from = n)

	roberta_merge_counts =
		data %>%
		filter(., score_model == 'ROBERTA') %>%
		group_by(., finality, created_dt) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(., names_from = finality, values_from = n)

	reddit <<- list()
	reddit$data <<- data
	reddit$boards <<- boards
	reddit$count_by_source_plot <<- count_by_source_plot
	reddit$count_by_model_plot <<- count_by_model_plot
	reddit$count_by_board_plot <<- count_by_board_plot
	reddit$distilbert_score_by_subreddit <<- distilbert_score_by_subreddit
	reddit$distilbert_merge_counts <<- distilbert_merge_counts
	reddit$roberta_merge_counts <<- roberta_merge_counts
})


## DISTILBERT By Board -----------------------------------------------------------
local({

	plot =
		reddit$data %>%
		filter(.,	score_model == 'DISTILBERT') %>%
		group_by(., created_dt, subreddit) %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		summarize(., mean_score = mean(score, na.rm = T), .groups = 'drop') %>%
		group_split(., subreddit) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
				) %>%
				mutate(., subreddit = x$subreddit[[1]], mean_score = coalesce(mean_score, 0)) %>%
				mutate(., mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'))
		) %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = subreddit))

	print(plot)
	reddit$distilbert_by_subreddit_plot <<- plot
})

## DISTILBERT Category --------------------------------------------------------
local({

	data =
		reddit$data %>%
		filter(.,	score_model == 'DISTILBERT')

	index_data =
		data %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		group_by(., created_dt, content_type) %>%
		summarize(., mean_score = mean(score), count = n(), .groups = 'drop') %>%
		filter(., count >= 5) %>%
		group_split(., content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')), x, by = 'created_dt'
				) %>%
				mutate(
					.,
					content_type = x$content_type[[1]], mean_score = zoo::na.locf(mean_score),
					count = ifelse(is.na(count), 0, count),
					mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'),
					mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
			)

	index_plot =
		index_data %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = content_type))

	reddit$distilbert_index_data <<- index_data
	reddit$distilbert_index_plot <<- index_plot
})

## ROBERTA Boards -----------------------------------------------------------
local({

	input_data =
		reddit$data %>%
		filter(., score_model == 'ROBERTA') %>%
		select(., score, created_dt, subreddit)

	subreddit_data_starts =
		input_data %>%
		group_by(., subreddit, score) %>%
		summarize(., min_dt = min(created_dt), .groups = 'drop')  %>%
		group_by(., subreddit) %>%
		summarize(., max_min_dt = max(min_dt))

	sent_counts_by_board_date =
		input_data %>%
		group_by(., score, created_dt, subreddit) %>%
		summarize(., sent_count = n(), .groups = 'drop') %>%
		ungroup(.) %>%
		group_split(., score, subreddit) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(
					filter(subreddit_data_starts, subreddit == x$subreddit[[1]])$max_min_dt[[1]],
					to = max(x$created_dt),
					by = '1 day'
					)),
				x,
				by = c('created_dt')
				) %>%
				mutate(
					.,
					score = x$score[[1]],
					subreddit = x$subreddit[[1]],
					sent_count = ifelse(is.na(sent_count), 0, sent_count),
					sent_count_7d = zoo::rollsum(sent_count, 7, fill = NA, na.pad = TRUE, align = 'right'),
					sent_count_14d = zoo::rollsum(sent_count, 14, fill = NA, na.pad = TRUE, align = 'right')
					)
		) %>%
		group_by(., created_dt, subreddit) %>%
		mutate(
			.,
			prop_7d = sent_count_7d/sum(sent_count_7d),
			prop_14d = sent_count_14d/sum(sent_count_14d)
			) %>%
		ungroup(.)

	plot =
		sent_counts_by_board_date %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_area(aes(x = created_dt, y = prop_14d, fill = score)) +
		geom_vline(aes(xintercept = as_date('2020-03-19'))) +
		geom_vline(aes(xintercept = as_date('2020-06-13'))) +

		facet_wrap(vars(subreddit))

	plot_neg_only =
		sent_counts_by_board_date %>%
		filter(., !score %in% c('joy', 'surprise', 'anger', 'neutral')) %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_area(aes(x = created_dt, y = prop_14d, fill = score)) +
		facet_wrap(vars(subreddit))

	plot_pos_only =
		sent_counts_by_board_date %>%
		filter(., score %in% c('joy', 'neutral', 'surprise')) %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_area(aes(x = created_dt, y = prop_14d, fill = score)) +
		facet_wrap(vars(subreddit))

	mood_affilliation = tribble(
		~ score, ~ mood_score,
		'joy', 1,
		'surprise', .5,
		'neutral', 0,
		'anger', -1,
		'disgust', -1,
		'sadness', -1,
		'fear', -1
	)

	print(plot)
	reddit$roberta_by_subreddit_data <<- sent_counts_by_board_date
	reddit$roberta_by_subreddit_plot <<- plot
	reddit$roberta_by_subreddit_plot_neg_only <<- plot_neg_only
	reddit$roberta_by_subreddit_plot_pos_only <<- plot_pos_only
	reddit$roberta_mood_affiliation <<- mood_affilliation
})

## ROBERTA Categories -----------------------------------------------------------
local({

	input_data =
		reddit$data %>%
		filter(., score_model == 'ROBERTA') %>%
		select(., score, created_dt, subreddit, content_type)

	subreddit_data_starts =
		input_data %>%
		group_by(., content_type, score) %>%
		summarize(., min_dt = min(created_dt), .groups = 'drop')  %>%
		group_by(., content_type) %>%
		summarize(., max_min_dt = max(min_dt))

	sent_counts_by_board_date =
		input_data %>%
		group_by(., score, created_dt, content_type) %>%
		summarize(., sent_count = n(), .groups = 'drop') %>%
		ungroup(.) %>%
		group_split(., score, content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(
					filter(subreddit_data_starts, content_type == x$content_type[[1]])$max_min_dt[[1]],
					to = max(x$created_dt),
					by = '1 day'
				)),
				x,
				by = c('created_dt')
			) %>%
				mutate(
					.,
					score = x$score[[1]],
					content_type = x$content_type[[1]],
					sent_count = ifelse(is.na(sent_count), 0, sent_count),
					sent_count_7d = zoo::rollsum(sent_count, 7, fill = NA, na.pad = TRUE, align = 'right'),
					sent_count_14d = zoo::rollsum(sent_count, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
		) %>%
		group_by(., created_dt, content_type) %>%
		mutate(
			.,
			prop_7d = sent_count_7d/sum(sent_count_7d),
			prop_14d = sent_count_14d/sum(sent_count_14d)
		) %>%
		ungroup(.)

	plot =
		sent_counts_by_board_date %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_area(aes(x = created_dt, y = prop_7d, fill = score)) +
		facet_wrap(vars(content_type))


	# Index
	index_data =
		input_data %>%
		inner_join(., reddit$roberta_mood_affiliation, by = 'score') %>%
		mutate(., score = mood_score) %>%
		select(., -mood_score) %>%
		group_by(., created_dt, content_type) %>%
		summarize(., mean_score = mean(score), count = n(), .groups = 'drop') %>%
		group_split(., content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
				) %>%
				mutate(
					.,
					content_type = x$content_type[[1]],
					mean_score = zoo::na.locf(mean_score),
					count = ifelse(is.na(count), 0, count),
					mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'),
					mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
		)

	index_plot =
		index_data %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = content_type))


	# Index with equal weighting between boards in dataset
	index_weighted_raw =
		input_data %>%
		inner_join(., reddit$roberta_mood_affiliation, by = 'score') %>%
		mutate(., score = mood_score) %>%
		select(., -mood_score) %>%
		group_by(., created_dt, subreddit, content_type) %>%
		summarize(., mean_score = mean(score), count = n(), .groups = 'drop') %>%
		group_split(., subreddit, content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
			) %>%
				mutate(
					.,
					subreddit = x$subreddit[[1]],
					content_type = x$content_type[[1]],
					mean_score = zoo::na.locf(mean_score),
					count = ifelse(is.na(count), 0, count),
					mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'),
					mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
		)

	index_weighted_raw %>%
		filter(., content_type == 'financial') %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_14dma, color = subreddit))

	index_weighted =
		index_weighted_raw %>%
		group_by(., content_type, created_dt) %>%
		summarize(
			.,
			n_components = n(),
			count = sum(count),
			mean_score = mean(mean_score),
			mean_score_7dma = mean(mean_score_7dma),
			mean_score_14dma = mean(mean_score_14dma),
			.groups = 'drop'
			)

	print(plot)
	reddit$roberta_by_category_data <<- sent_counts_by_board_date
	reddit$roberta_by_category_plot <<- plot
	reddit$roberta_index_data <<- index_data
	reddit$roberta_index_plot <<- index_plot
})

# Reddit Human Match Indices -----------------------------------------------------------

## Create Object -----------------------------------------------------------
local({

	reddit$human_indices <<- list()
})

## Labor Market -----------------------------------------------------------
local({

	boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE score_active = TRUE
		AND subreddit = 'jobs'
		--AND category IN ('labor_market')
		AND 1=1"
	)))


	input_data = dbGetQuery(db, str_glue(
		"WITH cte AS
		(
			SELECT
				r1.method AS source, r1.subreddit, DATE(r1.created_dttm AT TIME ZONE 'US/Eastern') AS created_dt, r1.ups,
				r1.name, b.category, title, selftext,
				ROW_NUMBER() OVER (PARTITION BY r1.name, r1.method ORDER BY scraped_dttm DESC) AS rn
			FROM sentiment_analysis_reddit_scrape r1
			INNER JOIN sentiment_analysis_reddit_boards b
				ON r1.subreddit = b.subreddit
			WHERE r1.method IN ('pushshift_all_by_board', 'top_1000_week_by_board', 'top_1000_month_by_board')
				AND DATE(created_dttm) >= '2019-01-01'
				AND r1.ups >= b.score_ups_floor
				--AND r1.subreddit IN 'jobs'
				AND category IN ('labor_market')
				LIMIT 100000
		)
		SELECT * FROM cte WHERE rn = 1"
		)) %>%
		as_tibble(.) %>%
		arrange(., created_dt) %>%
		as.data.table(.) %>%
		.[, text := paste0(title, selftext)] %>%
		.[, ind_type := fcase(
			str_detect(text, 'layoff|laid off|fired|unemployed|lost( my|) job'), 'layoff',
			str_detect(text, 'quit|resign|weeks notice|(leave|leaving)( a| my|)( job)'), 'quit',
			# str_detect(text, 'quit|resign|leave (a|my|) job'), 'quit',
			# One critical verb & then more tokenized
			str_detect(
				text,
				paste0(
					'hired|new job|background check|job offer|',
					'(found|got|landed|accepted|starting)( the| a|)( new|)( job| offer)',
					collapse = ''
				)
			),
			'hired',
			str_detect(text, 'job search|application|applying|rejected|interview|hunting'), 'searching',
			default = 'other'
		)]

	# Overlap verification
	input_data %>%
		group_by(., created_dt, source) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(
			.,
			id_cols = 'created_dt', values_from = n,
			names_from = source, names_prefix = 'source_'
		) %>%
		arrange(., desc(created_dt)) %>%
		print(., n = 20)


	ind_data =
		input_data %>%
		.[ind_type != 'other'] %>%
		# Get number of posts by ind_type and created_dt, filling in missing combinations with 0s
		.[, list(n_ind_type_by_dt = .N), by = c('created_dt', 'ind_type')] %>%
		merge(
			.,
			CJ(
				created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day'),
				ind_type = unique(.$ind_type)
			),
			by = c('created_dt', 'ind_type'),
			all.y = T
		) %>%
		.[, n_ind_type_by_dt := fifelse(is.na(n_ind_type_by_dt), 0, n_ind_type_by_dt)] %>%
		# Now merge to get number of total posts per day
		merge(
			.,
			.[, list(n_created_dt = sum(n_ind_type_by_dt)), by = 'created_dt'] %>%
				.[order(created_dt)] %>%
				.[, n_created_dt_7d := frollsum(n_created_dt, n = 7, algo = 'exact')] %>%
				.[, n_created_dt_14d := frollsum(n_created_dt, n = 14, algo = 'exact')] %>%
				.[, n_created_dt_30d := frollsum(n_created_dt, n = 30, algo = 'exact')],
			by = 'created_dt',
			all.x = T
		) %>%
		# Get 7-day count by ind_type
		.[order(created_dt, ind_type)] %>%
		.[, n_ind_type_by_dt_7d := frollsum(n_ind_type_by_dt, n = 7, algo = 'exact'), by = c('ind_type')] %>%
		.[, n_ind_type_by_dt_14d := frollsum(n_ind_type_by_dt, n = 14, algo = 'exact'), by = c('ind_type')] %>%
		.[, n_ind_type_by_dt_30d := frollsum(n_ind_type_by_dt, n = 30, algo = 'exact'), by = c('ind_type')] %>%
		# Calculate proportion for 7d rates
		.[, rate := n_ind_type_by_dt/n_created_dt] %>%
		.[, rate_7d := n_ind_type_by_dt_7d/n_created_dt_7d] %>%
		.[, rate_14d := n_ind_type_by_dt_14d/n_created_dt_14d] %>%
		.[, rate_30d := n_ind_type_by_dt_30d/n_created_dt_30d]

	ind_plot =
		ind_data %>%
		.[ind_type != 'other'] %>%
		hchart(., 'line', hcaes(x = created_dt, y = rate_14d, group = ind_type))

	ratios =
		ind_data %>%
		dcast(., created_dt ~ ind_type, value.var = c('rate_7d', 'rate_14d', 'rate_30d')) %>%
		.[, layoff_to_quit_14d := rate_14d_layoff/rate_14d_quit] %>%
		.[, layoff_to_quit_30d := rate_30d_layoff/rate_30d_quit] %>%
		.[, hired_quit_to_layoff_14d := (rate_14d_quit + rate_14d_hired)/rate_14d_layoff] %>%
		.[, hired_quit_to_layoff_30d := (rate_30d_quit + rate_30d_hired)/rate_30d_layoff] %>%
		melt(
			.,
			id.vars = 'created_dt',
			measure.vars = c(
				'layoff_to_quit_14d', 'layoff_to_quit_30d', 'hired_quit_to_layoff_14d', 'hired_quit_to_layoff_30d'
				),
			variable.name = 'ratio',
			value.name = 'value'
		)

	ratios_plot =
		ratios %>%
		hchart(., 'line', hcaes(x = created_dt, y = value, group = ratio))

	# Match reddit$distilbert_index_data and reddit$roberta_index_data format
	index_data =
		ind_data %>%
		dcast(., created_dt + n_created_dt ~ ind_type, value.var = c('rate', 'rate_7d', 'rate_14d', 'rate_30d')) %>%
		.[, mean_score := (rate_quit + rate_hired)/(rate_layoff + rate_quit + rate_hired)] %>%
		.[, mean_score_7dma := (rate_7d_quit + rate_7d_hired)/(rate_7d_layoff + rate_7d_quit + rate_7d_hired)] %>%
		# TEMP - Use 30d for 14dma
		.[, mean_score_14dma := (rate_30d_quit + rate_30d_hired)/(rate_30d_layoff + rate_30d_quit + rate_30d_hired)] %>%
		as_tibble(.) %>%
		transmute(
			.,
			created_dt,
			content_type = 'labor_market',
			count = n_created_dt,
			mean_score, mean_score_7dma, mean_score_14dma
		) %>%
		# Normalize to between -1 to 1
		mutate(., across(starts_with('mean_score'), function(x) (x * 2) - .5))


	index_plot =
		index_data %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = content_type))

	reddit$human_indices$labor_market$ind_plot <<- ind_plot
	reddit$human_indices$labor_market$ratios_plot <<- ratios_plot
	reddit$human_indices$labor_market$index_data <<- index_data
	reddit$human_indices$labor_market$index_plot <<- index_plot
})

## Bind --------------------------------------------------------
local({

	index_data = bind_rows(
		reddit$human_indices$labor_market$index_data
	)

	reddit$human_index_data <<- index_data
})

# Media --------------------------------------------------------

## Create Map --------------------------------------------------------
local({

	content_type_map = tribble(
		~ source, ~ category, ~ content_type,
		'ft', 'economics', 'financial'
	)

	media <<- list()
	media$content_type_map <<- content_type_map
})

## Pull Data -------------------------------------------------------------------
local({

	data = dbGetQuery(db, str_glue(
		"SELECT
			m1.source, m1.method AS category, m1.created_dt,
			m2.score_model, m2.score, m2.score_conf, m2.scored_dttm
		FROM sentiment_analysis_media_scrape m1
		INNER JOIN sentiment_analysis_media_score m2
			ON m1.id = m2.scrape_id
		WHERE text_part = 'all_text'
			AND score_model IN ('DISTILBERT', 'DICT', 'ROBERTA')
			AND created_dt >= '2019-01-01'"
		)) %>%
		as_tibble(.) %>%
		inner_join(., media$content_type_map, by = c('source', 'category')) %>%
		arrange(., created_dt)

	# Prelim Checks
	count_by_source_plot =
		data %>%
		group_by(., created_dt, score_model, source) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		arrange(., created_dt) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = n, color = score_model)) +
		facet_wrap(vars(source))

	media$data <<- data
	media$count_by_source_plot <<- count_by_source_plot
})

## DISTILBERT ---------------------------------------------------------------------
local({

	index_data =
		media$data %>%
		filter(., score_model == 'DISTILBERT') %>%
		group_by(., created_dt, content_type) %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		summarize(., mean_score = mean(score, na.rm = T), count = n(), .groups = 'drop') %>%
		group_split(., content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
			) %>%
				mutate(., content_type = x$content_type[[1]], mean_score = coalesce(mean_score, 0)) %>%
				mutate(., mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right')) %>%
				mutate(., mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right'))
		)

	index_plot =
		index_data %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = content_type))

	media$distilbert_index_data <<- index_data
	media$distilbert_index_plot <<- index_plot
})

## ROBERTA ---------------------------------------------------------------------
local({

	input_data =
		media$data %>%
		filter(., score_model == 'ROBERTA') %>%
		select(., score, created_dt, content_type)

	category_date_starts =
		input_data %>%
		group_by(., content_type, score) %>%
		summarize(., min_dt = min(created_dt), .groups = 'drop')  %>%
		group_by(., content_type) %>%
		summarize(., max_min_dt = max(min_dt))

	sent_counts_by_board_date =
		input_data %>%
		group_by(., score, created_dt, content_type) %>%
		summarize(., sent_count = n(), .groups = 'drop') %>%
		ungroup(.) %>%
		group_split(., score, content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(
					filter(category_date_starts, content_type == x$content_type[[1]])$max_min_dt[[1]],
					to = max(x$created_dt),
					by = '1 day'
				)),
				x,
				by = c('created_dt')
			) %>%
				mutate(
					.,
					score = x$score[[1]],
					content_type = x$content_type[[1]],
					sent_count = ifelse(is.na(sent_count), 0, sent_count),
					sent_count_7d = zoo::rollsum(sent_count, 7, fill = NA, na.pad = TRUE, align = 'right'),
					sent_count_14d = zoo::rollsum(sent_count, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
		) %>%
		group_by(., created_dt, content_type) %>%
		mutate(
			.,
			prop_7d = sent_count_7d/sum(sent_count_7d),
			prop_14d = sent_count_14d/sum(sent_count_14d)
		) %>%
		ungroup(.)

	plot =
		sent_counts_by_board_date %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_area(aes(x = created_dt, y = prop_7d, fill = score)) +
		facet_wrap(vars(content_type))

	# Index
	index_data =
		input_data %>%
		inner_join(., reddit$roberta_mood_affiliation, by = 'score') %>%
		mutate(., score = mood_score) %>%
		select(., -mood_score) %>%
		group_by(., created_dt, content_type) %>%
		summarize(., mean_score = mean(score), count = n(), .groups = 'drop') %>%
		filter(., count >= 5) %>%
		group_split(., content_type) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
			) %>%
				mutate(
					.,
					content_type = x$content_type[[1]],
					mean_score = zoo::na.locf(mean_score),
					count = ifelse(is.na(count), 0, count),
					mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'),
					mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
		)

	index_plot =
		index_data %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = content_type))

	print(plot)
	media$roberta_by_category_data <<- sent_counts_by_board_date
	media$roberta_by_category_plot <<- plot
	media$roberta_index_data <<- index_data
	media$roberta_index_plot <<- index_plot
})

# Create Indices --------------------------------------------------------

## Combine & Adjust Reddit Indices --------------------------------------------------------
local({

	LOGISTIC_STEEPNESS = 10

	# Note whether is final
	full_join(
		reddit$distilbert_merge_counts %>% rename(., distilbert_pushshift = pushshift, top_200_pushshift = top_200),
		reddit$roberta_merge_counts %>% rename(., roberta_pushshift = pushshift, top_200_roberta = top_200),
		by = 'created_dt'
		) %>%
		arrange(., created_dt) %>%
		transmute(
			.,
			created_dt,
			is_final = !is.na(distilbert_pushshift) & !is.na(roberta_pushshift)
		)

	# Combine & adjust Reddit indices
	combined_data =
		bind_rows(
			reddit$roberta_index_data %>% mutate(., model = 'ROBERTA'),
			reddit$distilbert_index_data %>% mutate(., model = 'DISTILBERT'),
			reddit$human_index_data %>% mutate(., model = 'HUMAN')
		) %>%
		group_by(., created_dt, content_type) %>%
		summarize(
			.,
			n_score_models = n(),
			count = sum(count),
			score = mean(mean_score),
			score_7dma = mean(mean_score_7dma),
			score_14dma = mean(mean_score_14dma),
			score_models = paste0(model, collapse = ','),
			.groups = 'drop'
			) %>%
		filter(., n_score_models >= 2) %>%
		filter(., !is.na(score) & !is.na(score_7dma))

	# Adjust to fix Jan. 2020 levels at ~ 50
	adjustments =
		combined_data %>%
		filter(., created_dt >= '2020-01-01' & created_dt <= '2020-01-31') %>%
		group_by(., content_type) %>%
		summarize(., mean_score = mean(score_7dma))

	adjusted_data =
		combined_data %>%
		left_join(., adjustments, by = 'content_type') %>%
		mutate(
			.,
			score_adj = score - mean_score,
			score_adj = 100/(1 + exp((-1 * LOGISTIC_STEEPNESS) * (score_adj - 0)))
		) %>%
		group_split(., content_type) %>%
		map_dfr(., function(x)
			x %>%
				arrange(., created_dt) %>%
				mutate(., score_adj_7dma = zoo::rollmean(score_adj, 7, fill = NA, na.pad = TRUE, align = 'right')) %>%
				mutate(., score_adj_14dma = zoo::rollmean(score_adj, 14, fill = NA, na.pad = TRUE, align = 'right'))
		) %>%
		inner_join(
			.,
			dbGetQuery(
				db,
				"SELECT id AS index_id, content_type FROM sentiment_analysis_indices WHERE source_type = 'reddit'"
				),
			by = 'content_type'
			) %>%
		filter(., created_dt >= as_date('2019-02-01') ) %>%
		na.omit(.) %>%
		transmute(
			.,
			date = created_dt,
			index_id,
			count,
			score,
			score_7dma,
			score_14dma,
			score_adj,
			score_adj_7dma,
			score_adj_14dma,
			created_at = now('US/Eastern')
			)

	ggplot(adjusted_data) +
		geom_line(aes(x = date, y = score_adj_7dma, color = as.factor(index_id)))

	reddit$final_index_data <<- adjusted_data
})

## Combine & Adjust Media Indices --------------------------------------------------------
local({

	LOGISTIC_STEEPNESS = 20

	combined_data =
		bind_rows(
			media$roberta_index_data %>% mutate(., model = 'ROBERTA'),
			media$distilbert_index_data %>% mutate(., model = 'DISTILBERT')
		) %>%
		group_by(., created_dt, content_type) %>%
		summarize(
			.,
			n_score_models = n(),
			count = sum(count),
			score = mean(mean_score),
			score_7dma = mean(mean_score_7dma),
			score_14dma = mean(mean_score_14dma),
			score_models = paste0(model, collapse = ','),
			.groups = 'drop'
		) %>%
		filter(., n_score_models == 2) %>%
		filter(., !is.na(score) & !is.na(score_7dma))

	# Adjust to fix Jan. 2022 levels at ~ 50
	adjustments =
		combined_data %>%
		filter(., created_dt >= '2022-01-01' & created_dt <= '2022-01-31') %>%
		group_by(., content_type) %>%
		summarize(., mean_score = mean(score_7dma))

	adjusted_data =
		combined_data %>%
		left_join(., adjustments, by = 'content_type') %>%
		mutate(
			.,
			score_adj = score - mean_score,
			score_adj = 100/(1 + exp((-1 * LOGISTIC_STEEPNESS) * (score_adj - 0)))
		) %>%
		group_split(., content_type) %>%
		map_dfr(., function(x)
			x %>%
				arrange(., created_dt) %>%
				mutate(., score_adj_7dma = zoo::rollmean(score_adj, 7, fill = NA, na.pad = TRUE, align = 'right')) %>%
				mutate(., score_adj_14dma = zoo::rollmean(score_adj, 14, fill = NA, na.pad = TRUE, align = 'right'))
		) %>%
		inner_join(
			.,
			dbGetQuery(
				db,
				"SELECT id AS index_id, content_type FROM sentiment_analysis_indices WHERE source_type = 'trad'"
				),
			by = c('content_type')
		) %>%
		filter(., created_dt >= as_date('2019-02-01') ) %>%
		na.omit(.) %>%
		transmute(
			.,
			date = created_dt,
			index_id,
			count,
			score,
			score_7dma,
			score_14dma,
			score_adj,
			score_adj_7dma,
			score_adj_14dma,
			created_at = now('US/Eastern')
		)

	ggplot(adjusted_data) +
		geom_line(aes(x = date, y = score_adj_7dma, color = as.factor(index_id)))

	media$final_index_data <<- adjusted_data
})


## Aggregate --------------------------------------------------------
local({

	index_data =
		bind_rows(reddit$final_index_data, media$final_index_data) %>%
		inner_join(
			.,
			dbGetQuery(
				db,
				"SELECT id AS index_id, CONCAT(source_type, '-', content_type) AS name
				FROM sentiment_analysis_indices"
				),
			by = 'index_id'
			)

	plot =
		ggplot(index_data) +
		geom_line(aes(x = date, y = score_adj_7dma, color = name))

	print(plot)
	index_data <<- index_data
})



# Benchmarking --------------------------------------------------------

## External Data --------------------------------------------------------
local({

	external_series = as_tibble(dbGetQuery(db, str_glue(
		'SELECT varname, fullname, pull_source, source_key FROM sentiment_analysis_benchmarks'
	)))

	data_raw =
		external_series %>%
		filter(., pull_source == 'fred') %>%
		purrr::transpose(.) %>%
		map_dfr(., function(x)
			get_fred_data(x$source_key, CONST$FRED_API_KEY) %>%
				transmute(., date, varname = x$varname, value) %>%
				filter(., date >= as_date('2019-01-01')) %>%
				arrange(., date)
		)

	data_calculated = bind_rows(
		data_raw %>%
			filter(., varname == 'sp500') %>%
			mutate(., varname = 'sp500tr30', value = (value/lag(value, 30) - 1) * 100) %>%
			na.omit(.)
	)

	data = bind_rows(data_raw, data_calculated)

	benchmarks <<- list()
	benchmarks$external_series <<- external_series
	benchmarks$external_series_values <<- data
})

## Combine --------------------------------------------------------
local({

	index_hc_series =
		index_data %>%
		mutate(., date = as.numeric(as.POSIXct(date)) * 1000) %>%
		group_split(., name) %>%
		imap(., function(x, i) list(
			name = x$name[[1]],
			data =
				x %>%
				arrange(., date) %>%
				transmute(., x = date, y = score_adj_7dma) %>%
				na.omit(.) %>%
				purrr::transpose(.) %>%
				lapply(., function(x) list(x = x[[1]], y = x[[2]]))
		))

	benchmark_hc_series =
		benchmarks$external_series_values %>%
		left_join(., benchmarks$external_series, by = 'varname') %>%
		mutate(., date = as.numeric(as.POSIXct(date)) * 1000) %>%
		group_split(., varname) %>%
		imap(., function(x, i) list(
			name = x$fullname[[i]],
			data =
				x %>%
				arrange(., date) %>%
				transmute(., x = date, y = value) %>%
				na.omit(.) %>%
				purrr::transpose(.) %>%
				lapply(., function(x) list(x = x[[1]], y = x[[2]]))
		))

	comparison_plot =
		highchart(type = 'stock') %>%
		purrr::reduce(index_hc_series, function(accum, x)
			hc_add_series(
				accum,
				name = x$name, data = x$data,
				type = 'line', lineWidth = 4, yAxis = 0, visible = F
			),
			.init = .
		) %>%
		purrr::reduce(benchmark_hc_series, function(accum, x)
			hc_add_series(
				accum,
				name = x$name, data = x$data,
				type = 'line', lineWidth = 4, dashStyle = 'shortdot', yAxis = 1, visible = F
			),
			.init = .
		) %>%
		hc_credits(
			enabled = T, position = list(align = 'right'), text = 'Data represents smoothed 7-day moving averages'
		) %>%
		hc_yAxis_multiples(
			list(title = list(text = 'Test'), opposite = T),
			list(title = list(text = 'Benchmark Data'), opposite = F)
		) %>%
		hc_legend(
			title = list(text =
			'<span>Top Sectors<br>
        <span style="font-style:italic;font-size:.7rem">(Click to Hide/Show)</span>
      </span>'
			),
			useHTML = TRUE, enabled = TRUE, align = 'left', layout = 'vertical',
			backgroundColor = 'rgba(232, 225, 235, .8)'
		) %>%
		hc_navigator(enabled = F) %>%
		hc_title(text = 'U.S. Consumer Spending by Sector') %>%
		hc_tooltip(valueDecimals = 1, valueSuffix = '%') %>%
		hc_scrollbar(enabled = FALSE)


	analysis_data =
		index_data %>%
		filter(., name %in% c('Social Media Financial Market Sentiment')) %>%
		transmute(., date = date, name, value = score_adj_7dma) %>%
		left_join(
			.,
			benchmarks$external_series_values %>%
				filter(., varname == 'sp500') %>%
				transmute(., date, sp500 = value),
			by = 'date'
		) %>%
		na.omit(.)

	analysis_data %>%
		mutate(
			.,
			today_index_change = value/lag(value, 1) - 1,
			yesterday_index_change = lag(value, 1)/lag(value, 2) - 1,
			today_sp500_change = sp500/lag(sp500, 1) - 1
		) %>%
		mutate(
			.,
			yesterday_index_change_sign = ifelse(yesterday_index_change < 0, -1, 1)
		) %>%
		group_by(., yesterday_index_change_sign) %>%
		summarize(., mean_today_sp500_change = mean(today_sp500_change, na.rm = T))

	analysis_data %>%
		mutate(
			.,
			today_index_change = value/lag(value, 1) - 1,
			yesterday_index_change = lag(value, 1)/lag(value, 7) - 1,
			today_sp500_change = sp500/lag(sp500, 1) - 1
		) %>%
		mutate(
			.,
			today_index_change_sign = ifelse(today_index_change < 0, -1, 1),
			yesterday_index_change_sign = ifelse(yesterday_index_change < 0, -1, 1),
			today_sp500_change_sign = ifelse(today_sp500_change < 0, -1, 1)
		) %>%
		group_by(., yesterday_index_change_sign, today_sp500_change_sign) %>%
		summarize(
			.,
			count = n(),
			mean_today_sp500_change = mean(today_sp500_change, na.rm = T),
			.groups = 'drop'
		) %>%
		na.omit(.)

	benchmarks$comparison_plot <<- comparison_plot
})

# SQL Inserts --------------------------------------------------------

## Index Data to SQL --------------------------------------------------------
local({

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_index_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		index_data %>%
		select(
			.,
			date, index_id, count,
			score, score_7dma, score_14dma,
			score_adj, score_adj_7dma, score_adj_14dma,
			created_at
			) %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/2000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_index_values',
				'ON CONFLICT (index_id, date) DO UPDATE SET
				count=EXCLUDED.count,
				score=EXCLUDED.score,
				score_7dma=EXCLUDED.score_7dma,
				score_14dma=EXCLUDED.score_14dma,
				score_adj=EXCLUDED.score_adj,
				score_adj_7dma=EXCLUDED.score_adj_7dma,
				score_adj_14dma=EXCLUDED.score_adj_14dma,
				created_at=EXCLUDED.created_at'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_index_values')$count)
	message('***** Rows Added: ', final_count - initial_count)

	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'sentiment-analysis-create-indices', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
			),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
})

## Benchmark to SQL ---------------------------------------------------------
local({

	data =
		benchmarks$external_series_values %>%
		filter(., date >= as_date('2019-01-01')) %>%
		mutate(., created_at = now('US/Eastern')) %>%
		na.omit(.)

	initial_count = as.numeric(dbGetQuery(
		db,
		'SELECT COUNT(*) AS count FROM sentiment_analysis_benchmark_values')$count
		)
	message('***** Initial Count: ', initial_count)

	sql_result =
		data %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/2000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_benchmark_values',
				'ON CONFLICT (varname, date) DO UPDATE SET
				value=EXCLUDED.value,
				created_at=EXCLUDED.created_at'
				) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_benchmark_values')$count)
	message('***** Rows Added: ', final_count - initial_count)

	create_insert_query(
		tribble(
			~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
			JOB_NAME, 'sentiment-analysis-create-indices-benchmarks', today(), 'job-success',
			toJSON(list(rows_added = final_count - initial_count))
		),
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		) %>%
		dbExecute(db, .)
})

## Content Type Breakdown ---------------------------------------------------------
local({

	message('*** Sending ROBERTA Category Breakdowns to SQL')

	reddit_data =
		reddit$roberta_by_category_data %>%
		# Only keep dates with non-zero component counts
		inner_join(
			.,
			reddit$roberta_by_category_data %>%
				group_by(., created_dt, content_type) %>%
				summarize(., n_sent_count_14d = sum(sent_count_14d), .groups = 'drop') %>%
				filter(., n_sent_count_14d > 0),
			by = c('created_dt', 'content_type')
		) %>%
		# Only keep those that match an existing index
		inner_join(
			.,
			dbGetQuery(
				db,
				"SELECT id AS index_id, content_type FROM sentiment_analysis_indices WHERE source_type = 'reddit'"
				),
			by = 'content_type'
		) %>%
		filter(., created_dt >= as_date('2019-02-01') ) %>%
		transmute(., date = created_dt, emotion = score, index_id, value = sent_count_14d) %>%
		na.omit(.) %>%
		mutate(., created_at = now('US/Eastern'))


	media_data =
		media$roberta_by_category_data %>%
		# Only keep dates with non-zero component counts
		inner_join(
			.,
			media$roberta_by_category_data %>%
				group_by(., created_dt, content_type) %>%
				summarize(., n_sent_count_14d = sum(sent_count_14d), .groups = 'drop') %>%
				filter(., n_sent_count_14d > 0),
			by = c('created_dt', 'content_type')
		) %>%
		# Only keep those that match an existing index
		inner_join(
			.,
			dbGetQuery(
				db,
				"SELECT id AS index_id, content_type FROM sentiment_analysis_indices WHERE source_type = 'trad'"
				),
			by = 'content_type'
		) %>%
		filter(., created_dt >= as_date('2019-02-01') ) %>%
		transmute(., date = created_dt, emotion = score, index_id, value = sent_count_14d) %>%
		na.omit(.) %>%
		mutate(., created_at = now('US/Eastern'))

	initial_count = as.numeric(
		dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_index_roberta_values')$count
	)

	message('***** Initial Count: ', initial_count)

	sql_result =
		bind_rows(reddit_data, media_data) %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/20000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_index_roberta_values',
				'ON CONFLICT (index_id, date, emotion) DO UPDATE SET
				value=EXCLUDED.value,
				created_at=EXCLUDED.created_at'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(
		dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_index_roberta_values')$count
	)
	message('***** Rows Added: ', final_count - initial_count)

})

## Subreddit Breakdown ---------------------------------------------------------
local({

	message('*** Sending ROBERTA Subreddit Breakdowns to SQL')

	reddit_data =
		reddit$roberta_by_subreddit_data %>%
		# Only keep dates with non-zero component counts
		inner_join(
			.,
			reddit$roberta_by_subreddit_data %>%
				group_by(., created_dt, subreddit) %>%
				summarize(., n_sent_count_14d = sum(sent_count_14d), .groups = 'drop') %>%
				filter(., n_sent_count_14d > 0),
			by = c('created_dt', 'subreddit')
		) %>%
		filter(., created_dt >= as_date('2019-02-01') ) %>%
		transmute(., date = created_dt, emotion = score, subreddit, value = sent_count_14d) %>%
		na.omit(.) %>%
		mutate(., created_at = now('US/Eastern'))

	initial_count = as.numeric(
		dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_subreddit_roberta_values')$count
	)

	message('***** Initial Count: ', initial_count)

	sql_result =
		reddit_data %>%
		mutate(., across(where(is.POSIXt), function(x) format(x, '%Y-%m-%d %H:%M:%S %Z'))) %>%
		mutate(., split = ceiling((1:nrow(.))/20000)) %>%
		group_split(., split, .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'sentiment_analysis_subreddit_roberta_values',
				'ON CONFLICT (subreddit, date, emotion) DO UPDATE SET
				value=EXCLUDED.value,
				created_at=EXCLUDED.created_at'
			) %>%
				dbExecute(db, .)
		) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}

	final_count = as.numeric(
		dbGetQuery(db, 'SELECT COUNT(*) AS count FROM sentiment_analysis_subreddit_roberta_values')$count
	)
	message('***** Rows Added: ', final_count - initial_count)
})


## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
