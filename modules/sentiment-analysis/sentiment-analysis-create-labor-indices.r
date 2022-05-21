# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'sentiment-analysis-labor-text'
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


# Pull Data ------------------------------------------------------------------
local({
	
	boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE score_active = TRUE
		AND category IN ('labor_market')"
	)))
	
	pushshift_data = dbGetQuery(db, str_glue(
		"SELECT
			r1.method AS source, r1.subreddit, DATE(r1.created_dttm AT TIME ZONE 'US/Eastern') AS created_dt, r1.ups,
			r1.name, b.category, title, selftext
		FROM sentiment_analysis_reddit_scrape r1
		INNER JOIN sentiment_analysis_reddit_boards b
			ON r1.subreddit = b.subreddit
		WHERE r1.method = 'pushshift_all_by_board'
			AND DATE(created_dttm) >= '2019-01-01'
			AND r1.ups >= b.score_ups_floor
			AND category IN ('labor_market')
			LIMIT 100000"
		)) %>%
		as.data.table(.) %>%
		.[, text := paste0(title, selftext)] %>%
		.[, ind_type := fcase(
			str_detect(text, 'layoff|laid off|fired|unemployed|lost my job'), 'layoff',
			str_detect(text, 'quit|resign|leave a job|leave my job'), 'quit',
			# One critical verb & then more tokenized
			str_detect(text, 'hired|new job|(found|got|landed)( the|a|)( new|)( job|offer)|starting a job|background check'),
			'hired',
			str_detect(text, 'job search|application|applying|rejected|interview'), 'searching',
			default = 'other'
		)]

	
	ind_data =
		pushshift_data %>%
		# .[ind_type != 'other'] %>%
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
		.[, n_ind_type_by_dt_14d := frollsum(n_ind_type_by_dt, n = 14, algo = 'exact'),by = c('ind_type')] %>%
		.[, n_ind_type_by_dt_30d := frollsum(n_ind_type_by_dt, n = 30, algo = 'exact'),by = c('ind_type')] %>%
		# Calculate proportion for 7d rates
		.[, rate_7d := n_ind_type_by_dt_7d/n_created_dt_7d] %>%
		.[, rate_14d := n_ind_type_by_dt_14d/n_created_dt_14d] %>%
		.[, rate_30d := n_ind_type_by_dt_30d/n_created_dt_30d]
		
	ind_data %>%
		.[ind_type != 'other'] %>%
		ggplot(.) +
		geom_line(aes(x = created_dt, y = rate_30d, color = ind_type)) 
	
	pushshift_data %>%
		.[, list(n_layoff_date = .N), by = c('created_dt', 'ind_layoff')] %>%
		.[, ind_layoff := fifelse(ind_layoff == T, 'ind_layoff_1', 'ind_layoff_0')] %>%
		dcast(., created_dt ~ ind_layoff, value.var = 'n_layoff_date') %>%
		merge(
			.,
			data.table(created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day')),
			by = 'created_dt',
			all.y = T
			) %>%
		.[order(created_dt)] %>%
		.[, c('ind_layoff_0', 'ind_layoff_1') := 
				lapply(.SD, function(x) replace_na(x, 0)), 
				.SDcols = c('ind_layoff_0', 'ind_layoff_1')
			] %>%
		.[, c('ind_layoff_0_7d', 'ind_layoff_1_7d') :=
			lapply(.SD, function(x) frollsum(x, n = 7, fill = NA, algo = 'exact', align = 'right')),
			.SDcols = c('ind_layoff_0', 'ind_layoff_1')
			] %>%
		.[, layoff_rate_7dma := ind_layoff_1_7d/(ind_layoff_0_7d + ind_layoff_1_7d)] %>%
		print(.) %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = layoff_rate_7dma))
	
	# Same but 30d
	pushshift_data %>%
		.[, list(n_layoff_date = .N), by = c('created_dt', 'ind_layoff')] %>%
		.[, ind_layoff := fifelse(ind_layoff == T, 'ind_layoff_1', 'ind_layoff_0')] %>%
		dcast(., created_dt ~ ind_layoff, value.var = 'n_layoff_date') %>%
		merge(
			.,
			data.table(created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day')),
			by = 'created_dt',
			all.y = T
		) %>%
		.[order(created_dt)] %>%
		.[, c('ind_layoff_0', 'ind_layoff_1') := 
				lapply(.SD, function(x) replace_na(x, 0)), 
			.SDcols = c('ind_layoff_0', 'ind_layoff_1')
		] %>%
		.[, c('ind_layoff_0_7d', 'ind_layoff_1_7d') :=
				lapply(.SD, function(x) frollsum(x, n = 30, fill = NA, algo = 'exact', align = 'right')),
			.SDcols = c('ind_layoff_0', 'ind_layoff_1')
		] %>%
		.[, layoff_rate_7dma := ind_layoff_1_7d/(ind_layoff_0_7d + ind_layoff_1_7d)] %>%
		print(.) %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = layoff_rate_7dma))
	
	# Same but 14d
	pushshift_data %>%
		.[, list(n_layoff_date = .N), by = c('created_dt', 'ind_layoff')] %>%
		.[, ind_layoff := fifelse(ind_layoff == T, 'ind_layoff_1', 'ind_layoff_0')] %>%
		dcast(., created_dt ~ ind_layoff, value.var = 'n_layoff_date') %>%
		merge(
			.,
			data.table(created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day')),
			by = 'created_dt',
			all.y = T
		) %>%
		.[order(created_dt)] %>%
		.[, c('ind_layoff_0', 'ind_layoff_1') := 
				lapply(.SD, function(x) replace_na(x, 0)), 
			.SDcols = c('ind_layoff_0', 'ind_layoff_1')
		] %>%
		.[, c('ind_layoff_0_7d', 'ind_layoff_1_7d') :=
				lapply(.SD, function(x) frollsum(x, n = 14, fill = NA, algo = 'exact', align = 'right')),
			.SDcols = c('ind_layoff_0', 'ind_layoff_1')
		] %>%
		.[, layoff_rate_7dma := ind_layoff_1_7d/(ind_layoff_0_7d + ind_layoff_1_7d)] %>%
		print(.) %>%
		ggplot(.) + 
		geom_line(aes(x = created_dt, y = layoff_rate_7dma))
	
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
			WHERE r1.method IN ('top_200_today_by_board', 'top_1000_month_by_board')
				AND text_part = 'all_text'
				AND score_model IN ('DISTILBERT', 'ROBERTA', 'DICT')
				AND DATE(created_dttm) >= '2019-01-01'
				AND r1.ups >= b.score_ups_floor
		)
		SELECT * FROM cte WHERE rn = 1"
	)) %>%
		as_tibble(.) %>%
		arrange(., created_dt)
	

	# 5/18/22 - Use all available recent_data, just dump anything that overlaps with Pushshift data
	# Overlap verification
	recent_data %>%
		mutate(., in_pushshift = name %in% pushshift_data$name) %>%
		group_by(., created_dt, in_pushshift) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		pivot_wider(., id_cols = 'created_dt', values_from = n, names_from = in_pushshift, names_prefix = 'in_pushshift_') %>%
		print(., n = 100)
	
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
	reddit$count_by_source_plot <<- count_by_model_plot
	reddit$count_by_model_plot <<- count_by_model_plot
	reddit$count_by_board_plot <<- count_by_board_plot
	reddit$distilbert_score_by_subreddit <<- distilbert_score_by_subreddit
	reddit$distilbert_merge_counts <<- distilbert_merge_counts
	reddit$roberta_merge_counts <<- roberta_merge_counts
})
