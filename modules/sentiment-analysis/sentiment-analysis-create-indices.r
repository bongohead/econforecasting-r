#' Sentiment Analysis with Dictionary Analysis & BERT

# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-create-indices'
EF_DIR = Sys.getenv('EF_DIR')
RESET_SQL = FALSE

## Cron Log ----------------------------------------------------------
if (interactive() == FALSE && rstudioapi::isAvailable(child_ok = T) == F) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	


# Data Prep --------------------------------------------------------

## Create Tables --------------------------------------------------------
local({
# if (RESET_SQL) {
# 	
# 	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_reddit CASCADE')
# 	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_score_reuters CASCADE')
# 	
# 	dbExecute(
# 		db,
# 		'CREATE TABLE sentiment_analysis_score_reddit (
# 		scrape_id INT NOT NULL,
# 		score_model VARCHAR(255) NOT NULL, 
# 		score INT NOT NULL,
# 		score_conf DECIMAL(20, 4) NULL,
# 		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
# 		PRIMARY KEY (scrape_id, score_model),
# 		CONSTRAINT sentiment_analysis_scrape_reddit_fk FOREIGN KEY (scrape_id)
# 				REFERENCES sentiment_analysis_scrape_reddit (id) ON DELETE CASCADE ON UPDATE CASCADE
# 		)'
# 	)
# 	
# 	dbExecute(
# 		db,
# 		'CREATE TABLE sentiment_analysis_score_reuters (
# 		scrape_id INT NOT NULL,
# 		score_model VARCHAR(255) NOT NULL, 
# 		score INT NOT NULL,
# 		score_conf DECIMAL(20, 4) NULL,
# 		scored_dttm TIMESTAMP WITH TIME ZONE NOT NULL,
# 		PRIMARY KEY (scrape_id, score_model),
# 		CONSTRAINT sentiment_analysis_score_reuters_fk FOREIGN KEY (scrape_id)
# 				REFERENCES sentiment_analysis_scrape_reuters (id) ON DELETE CASCADE ON UPDATE CASCADE
# 		)'
# 	)
# 	
# }
})	

## Pull Unscored Data --------------------------------------------------------
local({
	
	# # Pull data NULL
	# reddit_scored = dbGetQuery(db,
	# 	"(
	# 		SELECT
	# 			reddit1.method AS source,
	# 			reddit1.subreddit, DATE(reddit1.created_dttm) AS created_dt, reddit1.ups,
	# 			reddit2.*
	# 		FROM sentiment_analysis_scrape_reddit reddit1
	# 		INNER JOIN sentiment_analysis_score_reddit reddit2
	# 			ON reddit1.id = reddit2.scrape_id
	# 		)
	# 	UNION ALL
	# 	(
	# 		SELECT
	# 			'rereddit' AS source, rereddit1.subreddit, rereddit1.created_dt, rereddit1.ups,
	# 			rereddit2.*
	# 		FROM sentiment_analysis_scrape_rereddit rereddit1
	# 		INNER JOIN sentiment_analysis_score_rereddit rereddit2
	# 			ON rereddit1.id = rereddit2.scrape_id
	# 	)"
	# 	) %>%
	# 	as_tibble(.) 
	
	
	pushshift_scored = dbGetQuery(db,
		"SELECT
				r1.method AS source, r1.subreddit, DATE(r1.created_dttm) AS created_dt, r1.ups,
				r2.*
			FROM sentiment_analysis_scrape_reddit r1
			INNER JOIN sentiment_analysis_score_reddit r2
				ON r1.id = r2.scrape_id
			WHERE r1.method = 'pushshift_all_by_board'
				AND text_part = 'all_text'
				AND score_model = 'DISTILBERT'") %>%
		as_tibble(.) 
	
	
	media_scored = dbGetQuery(db, 
		"SELECT
			'media' AS source, m1.*, m2.*
		FROM sentiment_analysis_scrape_media m1
		INNER JOIN sentiment_analysis_score_media m2
			ON m1.id = m2.scrape_id"
		) %>%
		as_tibble(.) 

	# reddit_scored <<- reddit_scored
	pushshift_scored <<- pushshift_scored
	media_scored <<- media_scored
})


## Plot By Board -----------------------------------------------------------
local({
	
	plot =
		reddit_scored %>%
		group_by(., subreddit) %>%
		mutate(., subreddit_count = n()) %>%
		filter(., subreddit_count >= 1000 & created_dt >= as_date('2022-02-01')) %>%
		ungroup(.) %>%
		group_by(., created_dt, subreddit) %>%
		summarize(., mean_score = mean(score), .groups = 'drop') %>% 
		group_split(., subreddit) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
				) %>%
				mutate(., subreddit = x$subreddit[[1]], mean_score = coalesce(mean_score, 0)) %>%
				mutate(., mean_score_dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'))
			) %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = subreddit))
	
	print(plot)
	subreddit_plot <<- plot
})



## Plot By Category --------------------------------------------------------
local({
	
	board_mapping =	collect(tbl(db, sql('SELECT board AS subreddit, category FROM sentiment_analysis_reddit_boards')))

	input_data =
		reddit_scored %>%
		filter(
			.,
			source %in% c('top_200_today_by_board', 'top_1000_month_by_board', 'top_1000_year_by_board', 'rereddit'),
			score_model == 'DISTILBERT',
			score_conf > .7,
			created_dt >= as_date('2020-01-01'),
			subreddit %in% board_mapping$subreddit
			)

	# Weight each post by the relative amount of votes is got compared to the median value that day
	input_data %>%
		group_by(., subreddit, created_dt) %>%
		summarize(., subreddit_mean_score = mean(ups), .groups = 'drop') %>%
		mutate(
			.,
			subreddit_mean_scored_7dlma = zoo::rollmean(subreddit_mean_score, 7, fill =  NA, na.paid, align = 'right')
			) %>%
		print(., n = 100)
		
	index_data =
		input_data %>%
		group_by(., subreddit) %>%
		mutate(., subreddit_mean_score = mean(score)) %>%
		ungroup(.) %>%
		inner_join(., board_mapping, by = 'subreddit') %>%
		group_by(., created_dt, category) %>%
		summarize(., mean_score = mean(score), count_posts = n(), .groups = 'drop') %>%
		filter(., count_posts >= 10) %>%
		group_split(., category) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
				) %>%
				mutate(
					.,
				 category = x$category[[1]], mean_score = zoo::na.locf(mean_score),
					mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'),
					mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
		)
				
	
	index_data %>%
		filter(., category == 'Labor Market') %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score, color = category)) +
		geom_point(aes(x = created_dt, y = mean_score, color = category))
	
})

# Sentiment Index --------------------------------------------------------

## Reuters Index --------------------------------------------------------

local({
	
	board_mapping =	collect(tbl(db, sql('SELECT board AS subreddit, category FROM sentiment_analysis_reddit_boards')))
	
	input_data =
		reuters_scored

	index_data =
		input_data %>%
		group_by(., created_dt) %>%
		summarize(., mean_score = mean(score), count_posts = n(), .groups = 'drop') %>%
		mutate(., mean_score_7dma = zoo::rollmean(mean_score, 28, fill = NA, na.pad = TRUE, align = 'right'))

	
	index_data %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score_7dma)) +
		geom_point(aes(x = created_dt, y = mean_score_7dma))

	
	
})

# Finalize --------------------------------------------------------

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

