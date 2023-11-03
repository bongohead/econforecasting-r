#' TBD
#'

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

# Construct Indices --------------------------------------------------------

## Get Benchmarks ----------------------------------------------------------
local({

	api_key = Sys.getenv('FRED_API_KEY')

	input_sources = tribble(
		~varname, ~ hist_source_freq, ~ hist_source_key,
		'unemp', 'm', 'UNRATENSA',
		'ics', 'w', 'ICNSA',
		'ccs', 'w', 'CCNSA',
		'kclf', 'm', 'FRBKCLMCILA',
		'sent', 'm', 'UMCSENT',
		'sp500', 'd', 'SP500'
	)

	fred_data =
		input_sources %>%
		df_to_list	%>%
		map(., \(x) c(x$hist_source_key, x$hist_source_freq)) %>%
		get_fred_obs_async(., api_key, .obs_start = '2010-01-01', .verbose = T) %>%
		left_join(
			.,
			select(input_sources, 'varname', 'hist_source_key'),
			by = c('series_id' = 'hist_source_key'),
			relationship = 'many-to-one'
		) %>%
		select(., -series_id)

	benchmarks <<- fred_data
})

## Get Training Data --------------------------------------------------------
local({

	input_data = get_query(pg, str_glue(
	    "SELECT
	        a.post_id, a.label_key, a.label_value, a.label_rationale AS rationale,
	        b.title, b.selftext, b.source_board, DATE(created_dttm AT TIME ZONE 'US/Eastern') AS created_dt
	    FROM text_scraper_reddit_llm_scores_v2 a
	    INNER JOIN text_scraper_reddit_scrapes b
	        ON a.scrape_id = b.scrape_id
	        AND b.source_board IN ('careerguidance', 'jobs')
	    WHERE a.prompt_id = 'labor_market_v1'"
	))

	date_ranges_by_board =
		input_data %>%
		group_by(., source_board) %>%
		summarize(., min_dt = min(created_dt), max_dt = max(created_dt), .groups = 'drop')

	smoothed_values =
		input_data %>%
		group_by(., label_key, created_dt, label_value) %>%
		summarize(., n = n(), .groups = 'drop') %>%
		right_join(
			.,
			expand_grid(
				distinct(input_data, label_key, label_value),
				created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day'),
				),
			by = c('created_dt', 'label_key', 'label_value')
			) %>%
		mutate(., n = replace_na(n, 0)) %>%
		arrange(., created_dt) %>%
		mutate(
			.,
			n_roll = zoo::rollapply(
				n, width = 90,
				FUN = function(x) sum(1:90 * x),
				fill = NA,
				align = 'right'
				),
			.by = c('label_key', 'label_value')
			) %>%
		left_join(
			.,
			input_data %>%
				group_by(., created_dt) %>%
				summarize(., created_dt_count = n_distinct(post_id)) %>%
				arrange(., created_dt) %>%
				mutate(., created_dt_count = zoo::rollapply(
					created_dt_count,
					90,
					\(x) sum(1:90 * x),
					fill = NA,
					align = 'right'
					)),
			by = 'created_dt'
		)

	smoothed_values %>%
		ggplot() +
		geom_line(aes(x = created_dt, y = n_roll, color = label_value)) +
		facet_wrap(vars(label_key))

	raw_vals =
		smoothed_values %>%
		pivot_wider(
			.,
			names_from = c(label_key, label_value), values_from = n_roll,
			id_cols = c(created_dt, created_dt_count), values_fill = 0
			) %>%
		mutate(
			.,
			emp_ratio = employment_status_employed/created_dt_count,
			resign_ratio = recently_seperated_resigned/created_dt_count,
			fire_ratio = `recently_seperated_fired/laid off`/created_dt_count,
			ue_ratio = employment_status_unemployed/employment_status_employed,
			neg_ratio = (`employment_status_unemployed` + `recently_seperated_fired/laid off`)/(created_dt_count*2),
			pos_ratio = (
				2 * `recently_received_pay_increase_yes - significant off` +
				1 * `job_search_status_received offer/started new job` +
				.5 * `employment_status_employed` +
				-.5 * `employment_status_unemployed` +
				-2 * `recently_seperated_fired/laid off` +
				2 * `recently_seperated_resigned`
				)/(created_dt_count*5),
			) %>%
		transmute(., date = created_dt, pos_ratio) %>%
		arrange(., date) %>%
		pivot_longer(., cols = -date, names_to = 'varname', values_to = 'value') %>%
		filter(., !is.na(value))

	# Normalize
	raw_vals %>%
		bind_rows(
			.,
			select(benchmarks, date, varname, value) %>%
				filter(., varname %in% c('kclf')) %>%
				mutate(., value = 1 * (value - min(value))),
			# select(benchmarks, date, varname, value) %>%
			# 	filter(., varname %in% c('ccs', 'ics')) %>%
			# 	mutate(., value = (1/value)),
			select(benchmarks, date, varname, value) %>%
				filter(., varname %in% c('sp500')),
			# select(benchmarks, date, varname, value) %>%
			# 	filter(., varname %in% c('unemp')) %>%
			# 	mutate(., value = log(value, 10)),
			# select(benchmarks, date, varname, value) %>%
			# 	filter(., varname %in% c('sent'))
			) %>%
		group_split(., varname) %>%
		map(., \(x) mutate(x, value = value/head(filter(x, date >= '2022-01-01'), 1)$value)) %>%
		list_rbind() %>%
		filter(., date >= as_date('2019-01-01')) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname))

})

samples = get_query(pg, str_glue(
	"WITH t0 AS (
			SELECT
				a.scrape_id, a.post_id, a.title, a.selftext, a.created_dttm,
				ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
			FROM text_scraper_reddit_scrapes a
			WHERE
				a.scrape_board IN ('jobs', 'careerguidance')
				AND a.ups >= 10
		)
		SELECT * FROM t0 WHERE rn = 1 ORDER BY random() --LIMIT {general_sentiment_sample_size}"
)) %>%
	mutate(., text = paste0(title, '\n', selftext))

# df =
# 	samples %>%
# 	mutate(., title = str_to_lower(title), created_dt = as_date(created_dttm)) %>%
# 	mutate(., label = case_when(
# 		str_detect(text, 'layoff|laid off|fired|unemployed|lost( my|) job') ~ 'layoff',
# 		str_detect(text, 'quit|resign|weeks notice|(leave|leaving)( a| my|)( job)') ~ 'quit',
# 		# str_detect(text, 'quit|resign|leave (a|my|) job'), 'quit',
# 		# One critical verb & then more tokenized
# 		str_detect(
# 			text,
# 			paste0(
# 				'hired|new job|background check|job offer|',
# 				'(found|got|landed|accepted|starting)( the| a|)( new|)( job| offer)',
# 				collapse = ''
# 			)
# 		) ~ 'hired',
# 		str_detect(text, 'job search|application|applying|rejected|interview|hunting') ~ 'searching'
# 	)) %>%
# 	group_by(., created_dt, label) %>%
# 	summarize(., n = n(), .groups = 'drop') %>%
# 	right_join(
# 		.,
# 		expand_grid(
# 			label = unique(.$label),
# 			created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day'),
# 		),
# 		by = c('created_dt', 'label')
# 	) %>%
# 	mutate(., n = replace_na(n, 0)) %>%
# 	arrange(., created_dt) %>%
# 	mutate(., n_roll = zoo::rollsum(n, 30, fill = NA, align = 'right'), .by = c('label'))
#
# df %>%
# 	mutate(., label = replace_na(label, 'na')) %>%
# 	mutate(., total = sum(n_roll), .by = 'created_dt') %>%
# 	pivot_wider(., id_cols = c(created_dt, total), names_from = label, values_from = n_roll, values_fill = 0) %>%
# 	tail(., -60) %>%
# 	mutate(across(c(layoff, quit, hired, searching), \(x) x/total)) %>%
# 	select(., -total, -na) %>%
# 	pivot_longer(., cols = c(layoff, quit, hired, searching)) %>%
# 	ggplot +
# 	geom_line(aes(x = created_dt, y = value, color = name))


# samples = get_query(pg, str_glue(
# 	"WITH t0 AS (
# 			SELECT
# 				a.scrape_id, a.post_id, a.title, a.created_dttm,
# 				ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
# 			FROM text_scraper_reddit_scrapes a
# 			WHERE
# 				a.scrape_board IN ('Economics')
# 				AND a.ups >= 10
# 		)
# 		SELECT * FROM t0 WHERE rn = 1 ORDER BY random() --LIMIT {general_sentiment_sample_size}"
# ))

# df =
# 	samples %>%
# 	mutate(., title = str_to_lower(title), created_dt = as_date(created_dttm)) %>%
# 	mutate(., label = case_when(
# 		str_detect(title, 'house|real_estate') ~ 'housing',
# 		str_detect(title, 'expensive|inflation|cost of living|afford') ~ 'inflation',
# 		str_detect(title, 'stocks|stock market') ~ 'market',
# 		str_detect(title, 'unemploy|job') ~ 'labor_market',
# 	)) %>%
# 	group_by(., created_dt, label) %>%
# 	summarize(., n = n(), .groups = 'drop') %>%
# 	right_join(
# 		.,
# 		expand_grid(
# 			label = unique(.$label),
# 			created_dt = seq(min(.$created_dt), to = max(.$created_dt), by = '1 day'),
# 		),
# 		by = c('created_dt', 'label')
# 	) %>%
# 	mutate(., n = replace_na(n, 0)) %>%
# 	arrange(., created_dt) %>%
# 	mutate(., n_roll = zoo::rollsum(n, 60, fill = NA, align = 'right'), .by = c('label'))
#
# df %>%
# 	mutate(., label = replace_na(label, 'na')) %>%
# 	mutate(., total = sum(n_roll), .by = 'created_dt') %>%
# 	pivot_wider(., id_cols = c(created_dt, total), names_from = label, values_from = n_roll, values_fill = 0) %>%
# 	tail(., -60) %>%
# 	mutate(across(c(housing, labor_market, market, inflation), \(x) x/total)) %>%
# 	select(., -total, -na) %>%
# 	pivot_longer(., cols = c(housing, labor_market, market, inflation)) %>%
# 	ggplot +
# 	geom_line(aes(x = created_dt, y = value, color = name))
#
