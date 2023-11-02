#' Uses LLM to score a sample of scraped Reddit data; used for training a BERT model
#' See text-scraper-gtrain-py.py

# Initialize ----------------------------------------------------------
SAMPLE_SIZE = 2000
PROMPT_VERSION = 1

validation_log <<- list()
data_dump <<- list()

## Load Libs ----------------------------------------------------------
library(econforecasting)
library(tidyverse)
library(httr2)
library(rvest)
library(jsonlite, include.only = c('validate', 'toJSON', 'fromJSON'))
library(DBI, include.only = 'dbExecute')

## Load Connection Info ----------------------------------------------------------
load_env(Sys.getenv('EF_DIR'))
pg = connect_pg()

# Get Training Data --------------------------------------------------------

## Pull Samples  --------------------------------------------------------
local({

	# Only score each post from the source table once, even if duplicated in source table (across methods)
	# Anti join with existing scores on post_id to prevent re-scoring the same post_ids
	jobs_data =
		get_query(pg, str_glue(
			"WITH t0 AS (
				SELECT
					a.post_id,
					a.id AS scrape_id,
					a.title,
					a.selftext,
					a.created_dttm,
					ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
					ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
				FROM text_scraper_reddit_scrape a
				LEFT JOIN text_scraper_reddit_llm_scores b
					ON a.post_id = b.post_id
					AND b.prompt_version = {PROMPT_VERSION}
				WHERE
					b.id IS NULL
					AND a.source_board IN ('jobs', 'careerguidance')
					AND a.ups >= 10
					AND a.scrape_method IN ('top_200_today', 'top_1000_today', 'top_1000_week', 'top_1000_month', 'top_1000_year')
					AND a.selftext NOT IN ('', '[deleted]')
					AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) <= 1000
			)
			SELECT * FROM t0 WHERE rn = 1 ORDER BY random() LIMIT {SAMPLE_SIZE}
			"
		))

	jobs_data %>%
		count(., date = date(created_dttm)) %>%
		ggplot +
		geom_line(aes(x = date, y = n))

	jobs_data <<- jobs_data
})

## Prompts -----------------------------------------------------------------
local({

	label_options = tribble(
		~ key, ~ value,
		'employment_status', 'employed',
		'employment_status', 'unemployed',
		'employment_status', 'unknown',
		'recently_seperated', 'resigned',
		'recently_seperated', 'fired/laid off',
		'recently_seperated', 'considering seperation',
		'recently_seperated', 'no/unknown',
		'job_search_status', 'received offer/started new job',
		'job_search_status', 'searching/considering search',
		'job_search_status', 'not searching/unknown',
		'recently_received_pay_increase', 'yes - significant off',
		'recently_received_pay_increase', 'yes - small/unknown size',
		'recently_received_pay_increase', 'no/unknown',
		'includes_pay_complaint', 'yes',
		'includes_pay_complaint', 'no',
		) %>%
		mutate(., key_order = consecutive_id(key)) %>%
		mutate(., int_encode = 1:n() - 1, .by = key)

	opts_text =
		label_options %>%
		group_by(., key) %>%
		summarize(., key_order = unique(key_order), values = paste0(value, collapse = ', ')) %>%
		arrange(., key_order) %>%
		{paste0('- ', .$key, ': ', .$values)} %>%
		paste0(., collapse = '\n')

	prompts = list(
		system = str_glue(
		"Given a list of posts made on the job advice board of a social media site, return an array of objects in JSON format.
	    Each object should correspond to a single post. The keys to each object and their possible values should be:
		{opts_text}
		You may ONLY use these values. Return a rationale for each value as well."
		),
		user = list(
			"Started a new job and a company I applied to before just reached out and gave me an offer for 3 times my salary

			My current job is fine, but I'm underpaid. I like my boss and team, but I probably won't ever come back to the company.
			How should I approach leaving? I don't start the new job for another month, and I've accepted an offer at my new job.
			Do I tell my boss and team now?",
			"I basically went to college for nothing - Unemployed and Depressed.

			I got a Bachelors in Business Administration in Marketing a few years ago.
			I didn’t really take full advantage of being in school and preparing for the real world. Since graduating,
			I’ve submitted over 1300 applications to white collar jobs with multiple iterations of a resume, and have gotten
			only one offer at McDonald's but got fired.
			I usually apply to Marketing Coordinator roles or anything entry-level. At this point, I’m at a loss.",
			"Can someone explain what’s going on with the market?

			A few years ago, with less experience, I applied for a few jobs and got offers to 2 and they paid me to relocate.
			Now, I apply to 100 jobs to get a handful of first round interviews. Is the market really so horrible?",
			"Would I be absolutely stupid to quit my high paying job?

			I have a well paying job (low six figures) and I’ve been at my company almost a decade, but I'm burnt out from the politics.
			Why am I so terrified to quit? I feel like I’ve put so much time and energy into climbing high in the pay scale and wherever
			I end up is going to be just as bad."
		),
		assistant = list(
			list(
				employment_status = list('employed',  'The user states they have a "current job".'),
				recently_seperated = list('resigned',  'The user is resigning from their current job for a higher paying job.'),
				job_search_status = list('received offer/started new job',  'The user has received "an offer" at a new job.'),
				recently_received_pay_increase = list('yes - significant',  'The user accepted an offer for "3 times" their previous salary.'),
				includes_pay_complaint = list('yes',  'The user mentions they\'re currently "underpaid".')
			),
			list(
				employment_status = list('unemployed',  'The user states they are "Unemployed and Depressed".'),
				recently_seperated = list('fired/laid off',  'The user was laid off at McDonald\'s.'),
				job_search_status = list('searching/considering search',  'The user has submitted "over 1300 applications".'),
				recently_received_pay_increase = list('no/unknown',  'The user is unemployed and has no pay.'),
				includes_pay_complaint = list('no',  'The user makes no mention of pay or salary.')
			),
			list(
				employment_status = list('unknown',  'It is unclear whether the user is currently employed.'),
				recently_seperated = list('no/unknown',  'It is unclear whether the user recently left a job.'),
				job_search_status = list('searching/considering search',  'The user mentions applying to "100 jobs".'),
				recently_received_pay_increase = list('no/unknown',  'It is unclear whether the user recently received any pay increases.'),
				includes_pay_complaint = list('no',  'The user makes no mention of pay or salary.')
			),
			list(
				employment_status = list('employed', 'The user mentions currently having a "well paying job".'),
				recently_seperated = list('considering seperation', 'The user is considering leaving their current job.'),
				job_search_status = list('searching/considering search', 'The user is considering the possibility of getting a "job in a different field".'),
				recently_received_pay_increase = list('no/unknown', 'It is unclear whether the user recently received any pay increases.'),
				includes_pay_complaint = list('no', 'The user seems satisfied with their current pay.')
			)
		),
		user = list(
			"I'm burnt out but I need a new job

			This job search has got me on edge. I started applying in October with no offers from the 12+ places
			that I've interviewed for. I'm tired and taking a much-needed short vacation this week although I
			don't actually have the time. I currently have 10 tasks (reports, etc) on
			my plate. I got a measly raise which is truly a slap in the face for all the work that is currently on my shoulders.",
			"Job asked me to come back 3 months after layoff

			Yesterday, I got a call from the head of my old company asking if I would be interested in coming back. This was honestly the last thing I expected.
			I mentioned pay would be a big factor and they said they would be open to better compensation.
			They would also benefit from not having to train a new employee. Should I go back until I find something better?
			It would bring money while I have no income so Im leaning towards accepting."
		),
		assistant = list(
			list(
				employment_status = list('employed', 'The user is currently employed, as indicated by their mention of "10 tasks".'),
				recently_seperated = list('considering seperation', 'The user wants to leave their current job.'),
				job_search_status = list('searching/considering search', 'The user mentions they "started applying in October".'),
				recently_received_pay_increase = list('yes - small/unknown size', 'The user mentions they "got a measly raise".'),
				includes_pay_complaint = list('no', 'The user is dissatisfied with their pay, as they complained about "a measly raise".')
			),
			list(
				employment_status = list('unemployed', 'The user is unemployed and has "no income".'),
				recently_seperated = list('fired/laid off', 'The user was laid off 3 months ago.'),
				job_search_status = list('searching/considering search', 'The user seems to have been searching for jobs until they received this offer.'),
				recently_received_pay_increase = list('yes - small/unknown size',  'The user mentions the possibility of "better compensation", but does not specify how large it is.'),
				includes_pay_complaint = list('no', 'The user discusses compensation, but does not complain about it.')
			)
		)
		) %>%
		imap(., \(x, i) list(
			role = i,
			content = {
				if (i == 'system') str_replace_all(x, '\\t', '')
				else if (i == 'user') toJSON(
					map_chr(x, \(m)
						m %>%
							str_replace_all(., '\\t', '') %>%
							# Remove all linebreaks except double linebreaks
							str_replace_all(., '\n(?!\\n)', '_PLACEHOLDER_') %>%
							str_replace_all(., '\n\n', '\n') %>%
							str_replace_all(., '_PLACEHOLDER_', '')
					),
					auto_unbox = T
				)
				else toJSON(x, auto_unbox = T)
			}
		)) %>%
		unname(.)

	# Validate prompts
	validate_df =
		prompts %>%
		keep(\(x) x$role == 'assistant') %>%
		map(\(x) fromJSON(x$content, simplifyVector  = F)) %>%
		unlist(., recursive = F) %>%
		imap(., \(example, example_idx)
			 list_rbind(imap(example, \(m, j) tibble(idx = example_idx, key = j, value = m[[1]], rationale = m[[2]])))
		) %>%
		list_rbind(.)

	# Verify no NAs
	validate_df %>%
		count(., key, value) %>%
		inner_join(., label_options, by = c('key', 'value')) %>%
		print()

	prompts <<- prompts
	label_options <<- label_options
})

## LLM Calls  --------------------------------------------------------
local({

	base_prompt_tokens = 1600
	max_prompt_tokens = 3950
	user_prompt_tokens = max_prompt_tokens - base_prompt_tokens

	count_res = head(reduce(jobs_data$n_tokens, function(accum, c) {
		prev = tail(accum, 1)[[1]]
		res = {if (prev[[2]] + c >= user_prompt_tokens) c(prev[[1]] + 1, c) else c(prev[[1]], prev[[2]] + c)}
		return(c(accum, list(res)))
		},
		.init = list(c(1, 0)) # First val = group index, second val = token count
		), -1)

	job_data_to_score =
		jobs_data %>%
		mutate(
			.,
			score_group = map_int(count_res, \(x) x[1]),
			group_cum_token_count = map_dbl(count_res, \(x) x[2])
			)

	user_prompts =
		job_data_to_score %>%
		mutate(., user_message = paste0(title, '\n', str_replace_all(selftext, "\\t|\\n", " "))) %>%
		group_split(., score_group) %>%
		imap(., \(x, i) list(
			score_group = unique(x$score_group),
			scrape_ids = x$scrape_id,
			post_ids = x$post_id,
			user_message = toJSON(x$user_message, auto_unbox = T)
		))

	requests = map(user_prompts, \(p) {
		request('https://api.openai.com/v1/chat/completions') %>%
			req_headers(
				'Authorization' = paste0('Bearer ', Sys.getenv('OPENAI_API_KEY')),
				'Content-Type' = 'application/json'
			) %>%
			req_body_raw(toJSON(list(
				model = 'gpt-3.5-turbo',
				messages = c(
					prompts,
					list(list(role = 'user', content = p$user_message))
				)
			), auto_unbox = T)) %>%
			req_timeout(60 * 10)
		})

	http_responses = send_async_requests(requests, .chunk_size = 40, .max_retries = 4, .verbose = T)

	res = list_rbind(imap(http_responses, function(r, i) {

		response_content = resp_body_json(r)$choices[[1]]$message$content

		# Throw non-JSON outputs
		if (validate(response_content) == F) return(NULL)

		post_ids = user_prompts[[i]]$post_ids
		scrape_ids = user_prompts[[i]]$scrape_ids
		json_parsed = fromJSON(response_content, simplifyVector = F)

		# Throw out incorrect counts, since uncertain which one corresponds to what
		if (length(json_parsed) != length(post_ids)) return(NULL)

		# Iterate through each and check keys, pivot longer
		group_gpt_results = list_rbind(compact(imap(json_parsed, function(poster_res, j) {

			if (length(unique(label_options$key)) != length(names(poster_res))) return(NULL)

			if (!all(sort(unique(label_options$key)) == sort(unique(names(poster_res))))) return(NULL)

			if (!all(map(poster_res, \(k) length(k)) == 2)) return(NULL)

			list_rbind(imap(poster_res, \(k, name) tibble(key = name, value = k[[1]], rationale = k[[2]]))) %>%
				bind_cols(scrape_id = scrape_ids[[j]], post_id = post_ids[[j]], .)

			})))

		return(group_gpt_results)
	}))

	# Remove any post_ids where *any* of the label keys or values are invalid
	filtered_res =
		res %>%
		left_join(., transmute(label_options, key, value, label_encode = int_encode), by = c('key', 'value')) %>%
		mutate(., is_na = ifelse(is.na(label_encode), 1, 0)) %>%
		mutate(., post_invalid_keys_or_values = sum(is_na), .by = post_id) %>%
		filter(., post_invalid_keys_or_values == 0) %>%
		select(., -is_na, -post_invalid_keys_or_values)


	message('***** Unique post IDs returned: ', length(unique(filtered_res$post_id)))
	message('***** Unique post IDs desired: ', sum(map_int(user_prompts, \(x) length(x$post_ids))))

	llm_res <<- filtered_res
})

# Finalize --------------------------------------------------------

## Write Scores to SQL  --------------------------------------------------------
local({

	data_to_sql =
		llm_res %>%
		transmute(
			.,
			scrape_id,
			post_id,
			prompt_version = PROMPT_VERSION,
			label_key = key,
			label_value = value,
			label_rationale = rationale,
			label_encode
		)

	message(str_glue('*** Sending Media Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_reddit_llm_scores')
	message('***** Initial Count: ', initial_count)

	insert_groups =
		data_to_sql %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = F)

	insert_result = map_dbl(insert_groups, .progress = F, function(x)
		dbExecute(pg, create_insert_query(
			x,
			'text_scraper_reddit_llm_scores',
			'ON CONFLICT (post_id, prompt_version, label_key) DO UPDATE SET
				scrape_id=EXCLUDED.scrape_id,
				label_value=EXCLUDED.label_value,
				label_rationale=EXCLUDED.label_rationale,
				label_encode=EXCLUDED.label_encode'
		))
	)

	insert_result = {if (any(is.null(insert_result))) stop('SQL Error!') else sum(insert_result)}

	final_count = get_rowcount(pg, 'text_scraper_reddit_llm_scores')
	rows_added = final_count - initial_count
	message('***** Rows Added: ', rows_added)

	disconnect_db(pg)

	validation_log$post_label_combinations <<- rows_added
})
