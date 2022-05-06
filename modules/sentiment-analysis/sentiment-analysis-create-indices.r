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
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)

# Data Prep --------------------------------------------------------

## Create Tables --------------------------------------------------------
local({
if (RESET_SQL) {

	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_indices CASCADE')
	dbExecute(db, 'DROP TABLE IF EXISTS sentiment_analysis_index CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_indices (
			id SERIAL PRIMARY KEY,
			name VARCHAR(255) NOT NULL,
			source VARCHAR(255) NOT NULL,
			sector VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP
		)'
	)
	
	dbExecute(
		db,
		'CREATE TABLE sentiment_analysis_index_values (
			index_id INTEGER NOT NULL, 
			date DATE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE NULL DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (index_id, date),
			CONSTRAINT sentiment_analysis_index_values_fk FOREIGN KEY (index_id)
				REFERENCES sentiment_analysis_indices (id) ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)

}
})



# Reddit ------------------------------------------------------------------

## Pull Data ------------------------------------------------------------------
local({
	
	boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE scrape_active = TRUE"
	)))
	
	pushshift_data = dbGetQuery(db, str_glue(
		"SELECT
			r1.method AS source, r1.subreddit, DATE(r1.created_dttm) AS created_dt, r1.ups,
			r2.score_model, r2.score, r2.score_conf, r2.scored_dttm,
			b.category AS subreddit_category
		FROM sentiment_analysis_reddit_scrape r1
		INNER JOIN sentiment_analysis_reddit_score r2
			ON r1.id = r2.scrape_id
		INNER JOIN sentiment_analysis_reddit_boards b
			ON r1.subreddit = b.subreddit
		WHERE r1.method = 'pushshift_all_by_board'
			AND text_part = 'all_text'
			AND score_model IN ('DISTILBERT', 'DICT')"
		)) %>%
		as_tibble(.) %>%
		arrange(., created_dt)
	
	recent_data = dbGetQuery(db, str_glue(
		"WITH cte AS 
		(
			SELECT
				r1.method AS source, r1.subreddit, DATE(r1.created_dttm) AS created_dt, r1.ups,
				r2.score_model, r2.score, r2.score_conf, r2.scored_dttm,
				b.category AS subreddit_category,
				-- Get latest scraped value of post if multiple
				r1.name, r1.scraped_dttm, 
				ROW_NUMBER() OVER (PARTITION BY r1.name, r2.score_model ORDER BY scraped_dttm DESC) AS rn
			FROM sentiment_analysis_reddit_scrape r1
			INNER JOIN sentiment_analysis_reddit_score r2
				ON r1.id = r2.scrape_id
			INNER JOIN sentiment_analysis_reddit_boards b
				ON r1.subreddit = b.subreddit
			WHERE r1.method IN ('top_200_today_by_board', 'top_1000_month_by_board')
				AND text_part = 'all_text'
				AND score_model IN ('DISTILBERT', 'DICT')
		)
		SELECT * FROM cte WHERE rn = 1"
		)) %>%
		as_tibble(.) %>%
		arrange(., created_dt)
	
	# Get list of score_model x subreddit x created_dt combinations not already in pushshift_data
	to_keep =
		recent_data %>%
		group_by(., score_model, subreddit, created_dt) %>%
		summarize(., .groups = 'drop') %>%
		anti_join(
			.,
			pushshift_data %>%
				group_by(., score_model, subreddit, created_dt) %>% 
				summarize(., .groups = 'drop'),
			by = c('score_model', 'subreddit', 'created_dt')
		)
	
	kept_recent_data =
		recent_data %>%
		inner_join(., to_keep, by = c('score_model', 'subreddit', 'created_dt'))
	
	data = bind_rows(
		pushshift_data %>% mutate(., finality = 'pushshift'),
		kept_recent_data %>% mutate(., finality = 'top_200')
		)

	# Prelim Checks
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
	score_by_subreddit = 
		data %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		group_by(., created_dt, subreddit, score_model, finality) %>%
		summarize(., mean_score = mean(score), .groups = 'drop') %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score, color = score_model, linetype = finality)) +
		facet_wrap(vars(subreddit))
	
	
	# Merge Coutns
	merge_counts =
		data %>%
		filter(., score_model == 'DISTILBERT') %>%
		group_by(., finality, created_dt) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(., names_from = finality, values_from = n) 
	
	reddit <<- list()
	reddit$data <<- data
	reddit$boards <<- boards
	reddit$count_by_model_plot <<- count_by_model_plot
	reddit$count_by_board_plot <<- count_by_board_plot
	reddit$score_by_subreddit <<- score_by_subreddit
	reddit$merge_counts <<- merge_counts
})

## Plot By Board -----------------------------------------------------------
local({
	
	plot =
		reddit$data %>%
		filter(., score_model == 'DISTILBERT') %>%
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
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = subreddit))
	
	print(plot)
	reddit$index_by_subreddit_plot <<- plot
})

## Plot By Category --------------------------------------------------------
local({
	
	data =
		reddit$data %>%
		filter(
			.,
			score_model == 'DISTILBERT',
			score_conf > .7,
			created_dt >= as_date('2020-01-01')
		)
	
	index_data =
		data %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		group_by(., created_dt, subreddit_category) %>%
		summarize(., mean_score = mean(score), count_posts = n(), .groups = 'drop') %>%
		filter(., count_posts >= 5) %>%
		group_split(., subreddit_category) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')), x, by = 'created_dt'
				) %>%
				mutate(
					.,
					category = x$subreddit_category[[1]], mean_score = zoo::na.locf(mean_score),
					mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right')#,
					# mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
				)
			)
	
	index_plot =
		index_data %>%
		na.omit(.) %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = category))

	reddit$index_data <<- index_data
	reddit$index_plot <<- index_plot
})

## Monthly Aggregates --------------------------------------------------------
local({
	
	# monthly_data =
	# 	reddit$data %>%
	# 	filter(
	# 		.,
	# 		score_model == 'DISTILBERT',
	# 		score_conf > .7,
	# 		created_dt >= as_date('2020-01-01')
	# 	)
	# 
	# index_data =
	# 	data %>%
	# 	mutate(., score = ifelse(score == 'p', 1, -1)) %>%
	# 	group_by(., created_dt, subreddit_category) %>%
	# 	summarize(., mean_score = mean(score), count_posts = n(), .groups = 'drop') %>%
	# 	filter(., count_posts >= 5) %>%
	# 	group_split(., subreddit_category) %>%
	# 	map_dfr(., function(x)
	# 		left_join(
	# 			tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')), x, by = 'created_dt'
	# 		) %>%
	# 			mutate(
	# 				.,
	# 				category = x$subreddit_category[[1]], mean_score = zoo::na.locf(mean_score),
	# 				mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right')#,
	# 				# mean_score_14dma = zoo::rollmean(mean_score, 14, fill = NA, na.pad = TRUE, align = 'right')
	# 			)
	# 	)
	# 
	# index_plot =
	# 	index_data %>%
	# 	na.omit(.) %>%
	# 	ggplot(.) +
	# 	geom_line(aes(x = created_dt, y = mean_score_7dma, color = category))
	# 
	# reddit$index_data <<- index_data
	# reddit$index_plot <<- index_plot
})


# Media --------------------------------------------------------

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
			AND score_model IN ('DISTILBERT', 'DICT')"
	)) %>%
	as_tibble(.) %>%
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

	media <<- list()
	media$data <<- data
	media$count_by_source_plot <<- count_by_source_plot
})

## Indices ---------------------------------------------------------------------
local({
	
	index_data =
		media$data %>%
		filter(., score_model == 'DISTILBERT') %>%
		group_by(., created_dt, category) %>%
		mutate(., score = ifelse(score == 'p', 1, -1)) %>%
		summarize(., mean_score = mean(score, na.rm = T), .groups = 'drop') %>% 
		group_split(., category) %>%
		map_dfr(., function(x)
			left_join(
				tibble(created_dt = seq(min(x$created_dt), to = max(x$created_dt), by = '1 day')),
				x,
				by = 'created_dt'
			) %>%
				mutate(., category = x$category[[1]], mean_score = coalesce(mean_score, 0)) %>%
				mutate(., mean_score_7dma = zoo::rollmean(mean_score, 7, fill = NA, na.pad = TRUE, align = 'right'))
		)
	
	index_plot =
		index_data %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = mean_score_7dma, color = category))
	
	media$index_data <<- index_data
	media$index_plot <<- index_plot
})


# Benchmarking --------------------------------------------------------

## Financial Market Indices --------------------------------------------------------
local({
	
	# S&P 500
	sp500 =
		get_fred_data('SP500', CONST$FRED_API_KEY, .freq = 'd') %>%
		transmute(., date, sp500 = value) %>%
		filter(., date >= as_date('2018-01-01')) #%>%
		#mutate(., sp500 = (lead(sp500, 7)/sp500 - 1) * 100)

	index_hc_series =
		bind_rows(
			reddit$index_data,
			media$index_data %>% filter(., category == 'business' & created_dt >= as_date('2021-12-01'))
		) %>%
		mutate(., created_dt = as.numeric(as.POSIXct(created_dt)) * 1000) %>%
		group_split(., category) %>%
		imap(., function(x, i)
			list(
				name = x$category[[1]],
				yAxis = 0,
				data =
					x %>%
					arrange(., created_dt) %>%
					transmute(., x = created_dt, y = mean_score_7dma) %>%
					na.omit(.) %>%
					purrr::transpose(.) %>%
					lapply(., function(x) list(x = x[[1]], y = x[[2]]))
			)
		)
	
	market_plot =
		highchart(type = 'stock') %>%
		purrr::reduce(index_hc_series, function(accum, x)
			hc_add_series(
				accum,
				name = x$name, yAxis = x$yAxis, data = x$data,
				type = 'line', lineWidth = 4
			),
			.init = .
		) %>%
		hc_add_series(
			.,
			name = 'S&P 500 Returns',
			type = 'line',
			yAxis = 1,
			dashStyle = 'shortdot',
			data =
				sp500 %>%
				filter(., date >= min(reddit$index_data$created_dt)) %>%
				mutate(., date =  as.numeric(as.POSIXct(date)) * 1000) %>%
				na.omit(.) %>%
				purrr::transpose(.) %>%
				map(., ~ list(x = .[[1]], y = .[[2]]))
		) %>%
		hc_credits(
			enabled = T, position = list(align = 'right'), text = 'Data represents smoothed 7-day moving averages'
		) %>%
		hc_yAxis_multiples(
			list(title = list(text = 'Test'), opposite = T),
			list(title = list(text = 'Return'), opposite = F)
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
		reddit$index_data %>%
		filter(., subreddit_category %in% c('Financial Markets', 'Labor Market')) %>%
		transmute(., date = created_dt, category, value = mean_score_7dma) %>%
		left_join(., sp500, by = 'date')
	
	analysis_data %>%
		mutate(
			.,
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
			yesterday_index_change = lag(value, 1)/lag(value, 7) - 1,
			today_sp500_change = sp500/lag(sp500, 1) - 1
			) %>%
		mutate(
			.,
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

	benchmarks <<- list()
	benchmarks$financial_index_plot <<- market_plot
})




# Finalize --------------------------------------------------------

## Send to SQL --------------------------------------------------------

## Close Connections --------------------------------------------------------
dbDisconnect(db)
message(paste0('\n\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

