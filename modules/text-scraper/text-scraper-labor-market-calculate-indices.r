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
		'sent', 'm', 'UMCSENT'
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
	        AND b.source_board IN ('careerguidance')
	        AND b.scrape_method IN ('pushshift_backfill', 'top_1000_month', 'top_1000_year', 'top_1000_week')
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
		mutate(., n_roll = zoo::rollsum(n, 28, fill = NA, align = 'right'), .by = c('label_key', 'label_value'))

	smoothed_values %>%
		ggplot() +
		geom_line(aes(x = created_dt, y = n_roll, color = label_value)) +
		facet_wrap(vars(label_key))

	raw_vals =
		smoothed_values %>%
		pivot_wider(., names_from = c(label_key, label_value), values_from = n_roll, id_cols = created_dt) %>%
		mutate(
			.,
			emp_denom = employment_status_employed + employment_status_unknown + employment_status_unemployed,
			emp_ratio = employment_status_employed/emp_denom,
			resign_denom = `recently_seperated_resigned` + `recently_seperated_considering seperation` + `recently_seperated_fired/laid off` + `recently_seperated_no/unknown`,
			resign_ratio = recently_seperated_resigned/resign_denom,
			recently_seperated_denom = `recently_seperated_fired/laid off` + `recently_seperated_resigned` + `recently_seperated_considering seperation` + `recently_seperated_no/unknown`,
			fire_ratio = `recently_seperated_fired/laid off`/(recently_seperated_denom),
			ue_ratio = employment_status_unemployed/emp_denom,
			neg_ratio = (`employment_status_unemployed` + `recently_seperated_fired/laid off`)/(emp_denom + recently_seperated_denom),
			search_ratio = `job_search_status_received offer/started new job`/
				(`job_search_status_searching/considering search` + `job_search_status_received offer/started new job` + `job_search_status_not searching/unknown`)
			# pay_ratio = `includes_pay_complaint_yes`/
			# 	(`includes_pay_complaint_yes` + `includes_pay_complaint_no`)
			) %>%
		transmute(., date = created_dt, neg_ratio) %>%
		pivot_longer(., cols = -date, names_to = 'varname', values_to = 'value') %>%
		filter(., !is.na(value))

	# Normalize
	raw_vals %>%
		bind_rows(
			.,
			select(benchmarks, date, varname, value) %>%
				filter(., varname %in% c('ccs', 'ics', 'unemp'))
				) %>%
		group_split(., varname) %>%
		map(., \(x) mutate(x, value = value/filter(x, date == '2022-01-01')$value)) %>%
		list_rbind() %>%
		filter(., date >= as_date('2021-01-01')) %>%
		ggplot() +
		geom_line(aes(x = date, y = value, color = varname))

})

