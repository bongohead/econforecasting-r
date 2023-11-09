#' Backfill specific boards

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

# Construct Scorer-Level Indices --------------------------------------------------------

## Get Benchmarks ----------------------------------------------------------
local({

	api_key = Sys.getenv('FRED_API_KEY')

	input_sources = tribble(
		~varname, ~ hist_source_freq, ~ hist_source_key,
		'unemp', 'm', 'UNRATENSA',
		'ics', 'w', 'ICNSA',
		'ccs', 'w', 'CCNSA',
		'kclf', 'm', 'FRBKCLMCILA',
		'sent', 'm', 'UMCSENT'
	)

	fred_data =
		input_sources %>%
		df_to_list	%>%
		map(., \(x) {
				get_fred_obs_with_vintage(x$hist_source_key, api_key, x$hist_source_freq) %>%
				transmute(date, vdate = vintage_date, varname = x$varname, value)
			}) %>%
		list_rbind() %>%
		filter(., date >= '2017-01-01')

	benchmarks <<- fred_data
})

## LLM/BERT Scores --------------------------------------------------------
local({

	input_data = get_query(pg, str_glue(
	    "SELECT
	        a.post_id, a.label_key, a.label_value, a.label_rationale AS rationale,
	        b.title, b.selftext, b.source_board, DATE(created_dttm AT TIME ZONE 'US/Eastern') AS created_dt
	    FROM text_scraper_reddit_llm_scores_v2 a
	    INNER JOIN text_scraper_reddit_scrapes b
	        ON a.scrape_id = b.scrape_id
	    WHERE a.prompt_id = 'financial_health_v1'"
	))
	
	input_data %>%
		group_by(., created_dt, label_key, label_value) %>%
		summarize(., count = n(), .groups = 'drop') %>%
		mutate(., key_count = sum(count), .by = c('created_dt', 'label_key')) 
	
	wide_values =
		input_data %>%
		pivot_wider(
			.,
			id_cols = c('post_id', 'created_dt', 'source_board'),
			names_from = 'label_key',
			values_from = 'label_value'
		)
	
	is_res =
		wide_values %>%
		filter(source_board != 'jobs') %>%
		mutate(
			.,
			is_unemp = ifelse(employment_status == 'unemployed', 1, 0),
			is_emp = ifelse(employment_status == 'employed', 1, 0),
			is_emp_or_unemp = ifelse(employment_status %in% c('unemployed', 'employed'), 1, 0),
			is_pos = ifelse(financial_sentiment == 'strong', 1, 0),
			is_neg = ifelse(financial_sentiment == 'weak', 1, 0),
			is_neutral = ifelse(financial_sentiment == 'neutral', 1, 0),
			is_fired = ifelse(!is.na(unemployment_reason) & unemployment_reason == 'fired_or_laid_off', 1, 0),
			is_unsat = ifelse(!is.na(employment_satisfaction) & employment_satisfaction == 'unsatisfied', 1, 0)
		) %>%
		group_by(., created_dt) %>%
		summarize(
			.,
			across(starts_with('is_'), sum),
			count = n(),
			.groups = 'drop'
		) %>%
		arrange(., created_dt) 
	
	smoothed =
		is_res %>%
		mutate(across(c(starts_with('is_'), count), function(x)
			zoo::rollapply(
				x, width = 4*16,
				FUN = function(x) sum(.99^((length(x) - 1):0) * x),
				fill = NA, align = 'right'
			)
		)) %>%
		mutate(
			.,
			# unemp_ratio = is_unemp/is_emp_or_unemp,
			# pos_ratio = is_pos/(is_pos + is_neg + is_neutral),
			neg_ratio = is_neg/(count),
			fire_ratio = is_fired/is_emp_or_unemp,
			neg_ratio_2 = (is_fired + is_unemp + is_neg)/count,
			unsat_ratio = is_unsat/count
			) %>%
		select(., created_dt, contains('_ratio'))	%>%
		pivot_longer(., -c(created_dt), names_to = 'index', values_to = 'value', values_drop_na = T) %>%
		transmute(., date = created_dt, index, value) 
	
	smoothed %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = index, group = index))
	
		
		mutate(., score = case_when(
			recently_seperated %in% c('fired/laid off') & job_search_status != 'received offer/started new job' ~ -2,
			recently_received_pay_increase == 'yes - significant off' ~ 2,
			job_search_status == 'received offer/started new job ' ~ 2,
			recently_seperated %in% c('resigned') ~ 2,
			employment_status %in% c('employed') ~ .5,
			employment_status %in% c('unemployed') ~ -.5,
			TRUE ~ 0
		)) %>%
		group_by(., created_dt) %>%
		summarize(., score = sum(score), count = n()) %>%
		arrange(., created_dt) %>%
		# Cast into rolling sums
		mutate(across(c(count, score), function(col) {
			zoo::rollapply(col, width = 180, FUN = \(x) sum(.95^(179:0) * x), fill = NA, align = 'right')
		})) %>%
		mutate(., score_weight = score/count) %>%
		ggplot() +
		geom_line(aes(x = created_dt, y = score/count))
	
})

## Regex Scores --------------------------------------------------------
local({
	
		samples = get_query(pg, str_glue(
			"WITH t0 AS (
				SELECT
					a.scrape_id, a.post_id,
					a.source_board, a.scrape_method,
					a.title, a.selftext, a.ups,
					DATE(a.created_dttm) AS date,
					ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
					ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
				FROM text_scraper_reddit_scrapes a
				WHERE
					a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
					AND a.selftext IS NOT NULL
					-- 10 - 1000 words
					AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) BETWEEN 10 AND 1000
					AND (
						(DATE(a.created_dttm) BETWEEN '2018-01-01' AND '2022-12-31' AND scrape_method = 'pushshift_backfill')
						OR (DATE(a.created_dttm) BETWEEN '2023-01-01' AND '2023-08-20' AND scrape_method = 'pullpush_backfill')
						-- Allow low-upvoted posts from top 1k to be included to avoid skew of negative posts being most popular
						OR (
							DATE(a.created_dttm) BETWEEN '2023-01-01' AND '2023-11-01' 
							AND scrape_method = 'top_1000_year'
							AND ups <= 100
						)
						OR (
							DATE(a.created_dttm) > '2023-08-20'
							AND scrape_method IN ('top_1000_today', 'top_1000_week', 'top_1000_month')
						)
					)
			)
			SELECT * FROM t0 WHERE rn = 1 ORDER BY random()"
		))

		samples %>% count(., date) %>% ggplot + geom_line(aes(x = date, y = n))

		raw_counts =
			samples %>%
			# filter(., source_board == 'personalfinance') %>%
			mutate(
				.,
				text = paste0('TITLE: ', title, '\nPOST: ', str_replace_all(selftext, "\\t|\\n", " ")),
				is_layoff = ifelse(str_detect(text, 'layoff|laid off|fired|unemployed|lost( my|) job|laying off'), 1, 0),
				is_resign = ifelse(str_detect(text, 'quit|resign|weeks notice|(leave|leaving)( a| my|)( job)'), 1, 0),
				is_new_job = ifelse(str_detect(text, 'hired|new job|background check|job offer|(found|got|landed|accepted|starting)( the| a|)( new|)( job| offer)'), 1, 0),
				is_searching = ifelse(str_detect(text, 'job search|application|apply|reject|interview|hunt|resume'), 1, 0),
				is_inflation = ifelse(str_detect(text, 'inflation|cost-of-living|cost of living|expensive'), 1, 0),
				is_struggling = ifelse(str_detect(text, 'struggling|struggle|unemployed|fired|sad|angry|poor|poverty'), 1, 0),
				is_new_job_or_resign = ifelse(is_resign == 1 | is_new_job == 1, 1, 0),
				# is_debt = ifelse(str_detect(text, 'debt|credit card debt|loan'), 1, 0),
				# is_housing = ifelse(str_detect(text, 'housing|home|rent'), 1, 0)
				) %>%
			group_by(., date) %>%
			summarize(
				.,
				across(starts_with('is_'), sum),
				count = n(),
				.groups = 'drop'
				) %>%
			arrange(., date)

		smoothed_ratios =
			raw_counts %>%
			mutate(across(c(starts_with('is_'), count), function(x)
				zoo::rollapply(
					x, width = 4*8,
					FUN = function(x) sum(.99^((length(x) - 1):0) * x),
					fill = NA, align = 'right'
					)
				)) %>%
			pivot_longer(., -c(date, count), names_to = 'varname', values_to = 'value', values_drop_na = T) %>%
			mutate(., value = value/count) %>%
			transmute(., date, varname, value) 
		
		smoothed_ratios %>%
			ggplot() +
			geom_line(aes(x = date, y = value, color = varname, group = varname))
		
		# Create total sentiment index
		employment_index =
			smoothed_ratios %>%
			pivot_wider(., names_from = 'varname', values_from = 'value') %>% 
			mutate(., value = is_new_job_or_resign - is_layoff) %>% # domain is [-1, 1]
			# Scale to ~(0, 1)
			mutate(., value = 
			 	(value - mean(filter(., date >= '2018-06-01' & date <= '2018-12-31')$value))/
				mean(filter(., date >= '2020-01-01' & date <= '2020-06-01')$value) * 100
			) %>%
			transmute(., date, vdate = date + days(1), value, varname = 'employment_index') 
			
		employment_index %>%
			ggplot() +
			geom_line(aes(x = date, y = value))
		
		# sigmoid = function(x, L, k, x0) {
		# 	L/(1 + exp(-1 * k * (x - x0)))
		# }
		
		regex_subindices <<- bind_rows(total_sent_index)
})

## Create Indices ----------------------------------------------------------
local({
	
	total_sent_index =
		regex_smoothed %>%
		pivot_wider(., names_from = 'index', values_from = 'value') %>% 
		mutate(., value = is_layoff - is_new_job - is_resign) %>%
		transmute(., date, vdate = date + days(1), value, varname = 'employment_index') %>%
		# Filter so that 2018-2019 is mean 0
		# and 2019-2023 is mean 0
		mutate(., value = (value + .25) * 3)
	
	total_sent_index %>%
		ggplot() +
		geom_line(aes(x = date, y = value))
	
	
	total_sent_index 
	
	benchmarks
	
	# bind_rows(
	# 	regex_smoothed,
	# 	transmute(benchmarks, date, index = varname, value)
	# ) %>%
	# 	filter(., date >= as_date('2018-01-01')) %>%
	# 	filter(., !index %in% c('kclf', 'is_searching',  'sp500', 'sent', 'is_inflation')) %>%
	# 	mutate(., value = ifelse(index %in% c('unemp', 'ccs', 'ics'), log(value), value)) %>%
	# 	group_split(., index) %>%
	# 	map(., function(df) {
	# 		df %>%
	# 			mutate(., value = scale(.$value))
	# 		# mutate(., baseline = head(filter(., date > '2020-01-01'), 1)$value) 
	# 	}) %>%
	# 	list_rbind() %>%
	# 	mutate(., change = value) %>%
	# 	ggplot() + 
	# 	geom_line(aes(x = date, y = change, color = index, group = index), linewidth = .8)
	
	
})

# Construct Top-Level Indices ----------------------------------------------------

## Predictive Modeling ----------------------------------------------------
local({
	
	# Benchmarks:
	# 1. unemp_t(v=v*) = f(unemp_{t-1m})
	# 2. unemp_t(v=v*) = f(unemp_{t-1m}, ics_{t-1w}, ics_{t-2w}, ics_{t-3w}, ics_{t-4w})
	
	# Compare
	# 1. unemp_t(v=v*) = f(unemp_{t-1m}, ics_{t-1w}, ics_{t-2w}, ics_{t-3w}, ics_{t-4w}, employment_index_{t - 1d})	
	
	
	fdate = '2023-11-01'
	
	unemp_final_values = 
		filter(benchmarks, varname == 'unemp') %>%
		slice_max(., order_by = vdate, n = 1, by = date) %>%
		select(., date, value)
	
	train_fdates = tibble(
		fdate = seq(from = as_date('2018-01-01'), to = as_date('2023-10-01'), by = '1 month'),
		fc0date = fdate, # Nowcast
		fc1date = add_with_rollback(fc0date, months(1)) # T+1 forecast
		) %>%
		left_join(., transmute(unemp_final_values, fc0date = date, unemp.fc0 = value), by = 'fc0date') %>%
		left_join(., transmute(unemp_final_values, fc1date = date, unemp.fc1 = value), by = 'fc1date')
	
	# available_data = 
	# 	bind_rows(benchmarks, regex_subindices) %>%
	# 	filter(., vdate <= as_date(fdate)) %>%
	# 	group_by(., date, varname) %>%
	# 	slice_max(., order_by = vdate, n = 1) %>%
	# 	ungroup(.) %>%
	# 	pivot_wider(., id_cols = date, names_from = varname, values_from = value) 
	
	# Unemp lags refer to the months before the actual date
	####### TBD: Test alternative specifications of employer sentiment
	employment_index =
		smoothed_ratios %>%
		pivot_wider(., names_from = 'varname', values_from = 'value') %>% 
		# mutate(., value = is_layoff) %>%
		mutate(., value = is_new_job_or_resign - is_layoff) %>% # domain is [-1, 1]
		# Scale to ~(0, 1)
		mutate(., value = 
					 	(value - mean(filter(., date >= '2018-06-01' & date <= '2018-12-31')$value))/
					 	mean(filter(., date >= '2020-01-01' & date <= '2020-06-01')$value) * 100
		) %>%
		transmute(., date, vdate = date + days(1), value, varname = 'employment_index') 
	
	employment_index %>%
		ggplot() +
		geom_line(aes(x = date, y = value))
	##############
	
	
	############### WANT TO PRED UNEMPLOYMENT ROC, NOT LEVEL####################
	
	available_data_by_fdate_varname =
		train_fdates %>%
		select(., fdate) %>%
		cross_join(., bind_rows(benchmarks, employment_index)) %>%
		filter(., vdate <= fdate & fdate - years(1) <= vdate) %>%
		slice_max(., order_by = vdate, n = 1, by = c(fdate, date, varname)) # Get latest vdate for each date
	
	unemp_data_by_fdate_varname =
		available_data_by_fdate_varname %>%
		filter(., varname == 'unemp') %>%
		mutate(., lag_index = interval(date, fdate) %/% months(1)) %>%
		filter(., lag_index <= 4) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'unemp.l', names_sort = T)
	
	ics_data_by_fdate_varname =
		available_data_by_fdate_varname %>%
		filter(., varname == 'ics') %>%
		arrange(., date) %>%
		# Order lag index by # before occurance
		mutate(., lag_index = n():1, .by = fdate) %>%
		filter(., lag_index <= 8) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'ics.l', names_sort = T)
	
	empsent_data_by_fdate_varname =
		available_data_by_fdate_varname %>%
		filter(., varname == 'employment_index') %>%
		arrange(., date) %>%
		# Order lag index by # before occurance
		mutate(., lag_index = n():1, .by = fdate) %>%
		filter(., lag_index <= 8) %>%
		pivot_wider(., id_cols = fdate, names_from = lag_index, values_from = value, names_prefix = 'empsent.l', names_sort = T)
	
	list(train_fdates, unemp_data_by_fdate_varname, ics_data_by_fdate_varname, empsent_data_by_fdate_varname) %>%
		reduce(., \(a, b) left_join(a, b, by = 'fdate')) %>%
		lm(unemp.fc0 ~ unemp.l2 + ics.l1 + empsent.l1 - 1, data = .) %>%
		summary(.)
	
	########################
	
	unemp =
		available_data %>%
		select(., date, unemp) %>%
		na.omit(.) %>%
		mutate(., unemp_lag = log(unemp), unemp_lag.l1 = lag(unemp_lag, 1))
	
	train_fdates %>%
		left_join(., unemp, )
	
	summary(lm(unemp ~ unemp.l1, data = unemp))
	
	available_data %>%
		select(., date, ics) %>%
		na.omit(.) %>%
		add_lagged_columns(., cols = 'ics', max_lag = 4)

	available_data %>%
		filter(., varname == 'unemp') %>%
		pivot_wider(., id_cols = date, names_from = varname, values_from = value) %>%
		mutate(., unemp = unemp - dplyr::lag(unemp, 1))
	

	# Get latest historical data before each forecast date
	tibble(fdate = seq(from = as_date('2018-01-01'), to = as_date('2023-01-01'), by = '1 day')) %>%
		expand_grid(., varname = c('unemp', 'employment_index')) %>%
		left_join(., companies, join_by(company == id, closest(vdate <= fdate)))
	
	bind_rows(total_sent_index, fred_data)
})
