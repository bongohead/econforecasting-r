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


# Pull Data ------------------------------------------------------------------\

## Collect Data ------------------------------------------------------------------
local({
	
	boards = collect(tbl(db, sql(
		"SELECT subreddit, scrape_ups_floor FROM sentiment_analysis_reddit_boards
		WHERE score_active = TRUE
			AND subreddit = 'jobs'
			--AND category IN ('labor_market')
			AND 1=1"
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
			AND r1.subreddit = 'jobs'
			--AND category IN ('labor_market')
			LIMIT 100000"
		)) %>%
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

	
	ind_data =
		pushshift_data %>%
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
			measure.vars = c('layoff_to_quit_14d', 'layoff_to_quit_30d', 'hired_quit_to_layoff_14d', 'hired_quit_to_layoff_30d'),
			variable.name = 'ratio',
			value.name = 'value'
			)
	
	ratios %>%
		hchart(., 'line', hcaes(x = created_dt, y = value, group = ratio))

	ind_plot <<- ind_plot
	ratios_plot <<- ratios_plot
})
