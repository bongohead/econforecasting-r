#' Uses LLM to score a sample of scraped Reddit data; used for training a BERT model
#' See text-scraper-gtrain-py.py

# Initialize ----------------------------------------------------------
validation_log <<- list()
data_dump <<- list()

llm_outputs <<- list()

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


# Personal Finance ------------------------------------------------------

## Constants ---------------------------------------------------------------
local({
	financial_health_prompt_id <<- 'financial_health_v1'
	financial_health_sample_size <<- 1000000
})

## Pull Samples  --------------------------------------------------------
local({

	# Only score each post from the source table once, even if duplicated in source table (across methods)
	# Anti join with existing scores on post_id to prevent re-scoring the same post_ids
	samples = get_query(pg, str_glue(
		"WITH t0 AS (
			SELECT
				a.scrape_id, a.post_id,
				a.source_board, a.scrape_method,
				a.title, a.selftext,
				DATE(a.created_dttm) AS created_dt,
				ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
				ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
			FROM text_scraper_reddit_scrapes a
			LEFT JOIN text_scraper_reddit_llm_scores_v2 b
				ON a.post_id = b.post_id
				AND b.prompt_id = '{financial_health_prompt_id}'
			WHERE
				b.score_id IS NULL
				AND a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
				AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) BETWEEN 10 AND 1000
		)
		SELECT * FROM t0 WHERE rn = 1 ORDER BY random() 
		LIMIT {financial_health_sample_size}"
	))

	samples %>% count(., date = created_dt) %>% ggplot + geom_line(aes(x = date, y = n))
	
	regex_res = 
		samples %>%
		mutate(
			., 
			text = paste0('TITLE: ', title, '\nPOST: ', str_replace_all(selftext, "\\t|\\n", " ")),
			is_layoff = ifelse(str_detect(text, 'layoff|laid off|fired|unemployed|lost( my|) job|laying off'), 1, 0),
			is_resign = ifelse(str_detect(text, 'quit|resign|weeks notice|(leave|leaving)( a| my|)( job)'), 1, 0),
			is_new_job = ifelse(str_detect(text, 'hired|new job|background check|job offer|(found|got|landed|accepted|starting)( the| a|)( new|)( job| offer)'), 1, 0),
			is_searching = ifelse(str_detect(text, 'job search|application|applying|rejected|interview|hunting'), 1, 0),
			is_inflation = ifelse(str_detect(text, 'inflation|cost-of-living|cost of living|expensive'), 1, 0)
			# is_housing = ifelse(str_detect(text, 'housing|home|rent'), 1, 0)
			) %>%
		group_by(., created_dt) %>%
		summarize(
			., 
			across(starts_with('is_'), sum),
			count = n(),
			.groups = 'drop'
			) %>%
		arrange(., created_dt)
	
	regex_res %>%
		mutate(across(c(starts_with('is_'), count), function(x)
			zoo::rollapply(
				x, width = 360,
				FUN = function(x) sum(.99^((length(x) - 1):0) * x),
				fill = NA, align = 'right'
				)
			)) %>%
		pivot_longer(., -c(created_dt, count), names_to = 'name', values_to = 'value', values_drop_na = T) %>%
		mutate(., ratio = value/count) %>%
		ggplot() +
		geom_line(aes(x = created_dt, y = ratio, color = name, group = name))

	financial_health_samples <<- samples
})

## Prompts -----------------------------------------------------------------
local({
	
	# label_options = list(
	# 	employment_status = c('employed', 'unemployed', 'unknown'),
	# 	recently_seperated = c('resigned', 'fired/laid off', 'considering resigning', 'no', 'unknown'),
	# 	considering_seperation = c('considering', 'not considering', 'no')
	# )
	
	custom_function = '[
  {
    "name": "classify_posts",
    "description": "Classifies content and sentiment of a variety of society media posts for a personal finance forum",
    "parameters": {
      "type": "object",
      "properties": {
        "posts": {
          "type": "array",
          "description": "An array of objects representing posts",
          "items": {
            "financially_distressed": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family is financially stressed (e.g. struggling with debt or bills, bankruptcy)"
            },
            "financially_strong": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family is financially strong OR improving (e.g. pay raise, large gift)"
            },
            "income_increase": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family recently received an increase in income (e.g. pay raise, higher-paying job)"
            },
            "struggling_with_debt": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family is struggling with their debt load (e.g. credit cards, mortgages)"
            },
            "recently_terminated": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family recently was laid off or fired"
            },
            "recently_resigned": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family recently left a job voluntarily (not including retirement)"
            },
            "inflation": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family is thinking about or is concerned with inflation or cost of living"
            },
            "housing": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family is thinking about or is concerned with housing or renting"
            },
            "salary": {
              "type": "boolean",
              "description": "Does the post indicate the user or their family is thinking about or is concerned with pay or salary"
            }
          }
        }
      }
    }
  }]' 
	
	list(
		financial_distressed = F, financially_strong = F, 
		income_increase = F, struggling_with_debt = F,
		recently_terminated = F, recently_resigned = F,
		inflation = F, housing = F, salary = F
	)
	
	user_prompts =
		financial_health_samples %>%
		sample_n(., 5) %>%
		mutate(., user_message = paste0('*POST #', 1:nrow(.), '*\nTITLE: ', title, '\nPOST: ', str_replace_all(selftext, "\\t|\\n", " "))) %>%
		mutate(., score_group = 1) %>%
		group_split(., score_group) %>%
		imap(., \(x, i) list(
			score_group = unique(x$score_group),
			scrape_ids = x$scrape_id,
			post_ids = x$post_id,
			user_message = paste0(x$user_message, collapse = '\n\n')
		))
	
	requests = map(user_prompts, \(p) {
		request('https://api.openai.com/v1/chat/completions') %>%
			req_headers(
				'Authorization' = paste0('Bearer ', Sys.getenv('OPENAI_API_KEY')),
				'Content-Type' = 'application/json'
			) %>%
			req_body_raw(str_glue(
			'{{
				"model": "gpt-3.5-turbo",
				"messages": {messages_list},
				"temperature": 0.2,
				"functions": {functions},
				"function_call": "auto"
			}}', 
			messages_list = jsonlite::toJSON(list(list(role = 'user', content = p$user_message)), auto_unbox = T), 
			functions = str_replace_all(custom_function, '\\t|\\n', '')
			)) %>%
			req_timeout(60 * 2)
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
			
			if (length(unique(labor_market_label_options$key)) != length(names(poster_res))) return(NULL)
			
			if (!all(sort(unique(labor_market_label_options$key)) == sort(unique(names(poster_res))))) return(NULL)
			
			if (!all(map(poster_res, \(k) length(k)) == 2)) return(NULL)
			
			list_rbind(imap(poster_res, \(k, name) tibble(key = name, value = k[[1]], rationale = k[[2]]))) %>%
				bind_cols(scrape_id = scrape_ids[[j]], post_id = post_ids[[j]], .)
			
		})))
		
		return(group_gpt_results)
	}))
	
	

	base_prompts = list(
		system = str_glue(
			"Given a list of posts made on the job advice board of a social media site, return an array of objects in JSON format.
	    Each object should correspond to a single post. The keys to each object and their possible values should be:
		{opts_text}
		You may ONLY use these values. Return a rationale for each value as well."
		),
		user = 
"*POST #1*
TITLE: Word Of Warning
POST: So this post is not to garner sympathy or anything like that, I've learned from my mistake I'm just hoping others can learn as well. I was in a bad place emotionally and financially, I took a Payday loan for 150 dollars, now I could have paid it off from my first paycheck but I didn't I let payments get made instead. But then I checked my loan account and I was paying 500 dollars on a 150 dollar loan. I lost my job couldn't pay the rest off so just let the payments come.

*POST #2*
TITLE: Company offering promotion with no raise
POST: Hey everyone,  I am being offered a promotion however the hiring manager said they cannot do anything for me salary wise.  However, the catch is in a year they will promote me to a supervisor once I've gotten comfortable in the position then we can discuss compensation then.  So all I get is this hypothetical Carrot being dangled in front of me. My question is can I ask for some kind of clause in my contract or whatever I sign that 1 year from date of start I will be given a 20% raise with the duties of XYZ position blah blah blah?

*POST #3*
TITLE: I was recently given $14k. Pay off my car or invest?
POST: I make 74k a year before taxes  I had 18k in my checking account. I recently inherited 14k from a generous relative passing away, bringing me to 32k. I have 16k left on my car at something like 2.8 API. This is my only debt. Should I put that money into stocks or some kind of investing account? or just pay off my car loan?

*POST #4*
TITLE: Looking for an app to track my funds and budget
POST: Hello, I am looking for an easy to use app to track all my funds and budget. Primarily, I do not want to link any of my bank or investment accounts to it and am okay with entering them in manually. 

*POST #5*
TITLE: [US] I've been living paycheck-to-paycheck all of my life. How can I stop living like this?
POST: Let me provide some background on me:  I earn $65k/yr &amp; I of course have bills including a lot of credit card debt. The way I've always functioned is I split my bills in 1/2 to pay them when I get paid &amp; then whatever's leftover is spending $ for the week. I do have difficulty saving &amp; I was told it's probably due to my ADHD. I have no idea how to stop this living from paycheck-to-paycheck life &amp; was looking for advice on how to do so.
",
		assistant = list(
			list(
				financial_distressed = T, financially_strong = F, 
				income_increase = F, struggling_with_debt = T,
				recently_terminated = T, recently_resigned = F,
				inflation = F, housing = F, salary = F
			),
			list(
				financial_distressed = F, financially_strong = F, 
				income_increase = F, struggling_with_debt = F,
				recently_terminated = F, recently_resigned = F,
				inflation = F, housing = F, salary = T
			),
			list(
				financial_distressed = F, financially_strong = T, 
				income_increase = F, struggling_with_debt = F,
				recently_terminated = F, recently_resigned = F,
				inflation = F, housing = F, salary = F
			),
			list(
				financial_distressed = F, financially_strong = F, 
				income_increase = F, struggling_with_debt = F,
				recently_terminated = F, recently_resigned = F,
				inflation = F, housing = F, salary = F
			),
			list(
				financial_distressed = T, financially_strong = F, 
				income_increase = F, struggling_with_debt = T,
				recently_terminated = F, recently_resigned = F,
				inflation = F, housing = F, salary = F
			),

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
		base_prompts %>%
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
	
	labor_market_base_prompts <<- base_prompts
	labor_market_label_options <<- label_options
})

# Labor Market Data --------------------------------------------------------

## Constants ---------------------------------------------------------------
local({
	labor_market_prompt_id <<- 'labor_market_v1'
	labor_market_sample_size <<- 2000
})

## Pull Samples  --------------------------------------------------------
local({

	# Only score each post from the source table once, even if duplicated in source table (across methods)
	# Anti join with existing scores on post_id to prevent re-scoring the same post_ids
	samples = get_query(pg, str_glue(
		"WITH t0 AS (
			SELECT
				a.scrape_id, a.post_id,
				a.title, a.selftext,
				a.created_dttm,
				ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
				ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
			FROM text_scraper_reddit_scrapes a
			LEFT JOIN text_scraper_reddit_llm_scores_v2 b
				ON a.post_id = b.post_id
				AND b.prompt_id = '{labor_market_prompt_id}'
			WHERE
				b.score_id IS NULL
				AND a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
				AND a.selftext IS NOT NULL
				AND a.ups >= 10
				AND a.scrape_method IN (
					'pushshift_backfill', 'top_200_today', 'top_1000_today',
					'top_1000_week', 'top_1000_month', 'top_1000_year'
					)
				AND a.selftext NOT IN ('', '[deleted]', '[removed]')
				AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) <= 1000
		)
		SELECT * FROM t0 WHERE rn = 1 ORDER BY random() LIMIT {labor_market_sample_size}"
	))

	samples %>% count(., date = date(created_dttm)) %>% ggplot + geom_line(aes(x = date, y = n))

	labor_market_samples <<- samples
})


## Prompts -----------------------------------------------------------------
local({

	# label_options = list(
	# 	employment_status = c('employed', 'unemployed', 'unknown'),
	# 	recently_seperated = c('resigned', 'fired/laid off', 'considering resigning', 'no', 'unknown'),
	# 	considering_seperation = c('considering', 'not considering', 'no')
	# )

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
		mutate(., key_order = consecutive_id(key))

	opts_text =
		label_options %>%
		group_by(., key) %>%
		summarize(., key_order = unique(key_order), values = paste0(value, collapse = ', ')) %>%
		arrange(., key_order) %>%
		{paste0('- ', .$key, ': ', .$values)} %>%
		paste0(., collapse = '\n')

	base_prompts = list(
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
		base_prompts %>%
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

	labor_market_base_prompts <<- base_prompts
	labor_market_label_options <<- label_options
})

## LLM Calls  --------------------------------------------------------
local({

	base_prompt_tokens = 1600
	max_prompt_tokens = 3950
	user_prompt_tokens = max_prompt_tokens - base_prompt_tokens

	count_res = head(reduce(labor_market_samples$n_tokens, function(accum, c) {
		prev = tail(accum, 1)[[1]]
		res = {if (prev[[2]] + c >= user_prompt_tokens) c(prev[[1]] + 1, c) else c(prev[[1]], prev[[2]] + c)}
		return(c(accum, list(res)))
		},
		.init = list(c(1, 0)) # First val = group index, second val = token count
		), -1)

	# Batch
	data_to_score =
		labor_market_samples %>%
		mutate(
			.,
			score_group = map_int(count_res, \(x) x[1]),
			group_cum_token_count = map_dbl(count_res, \(x) x[2])
			)

	user_prompts =
		data_to_score %>%
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
					labor_market_base_prompts,
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

			if (length(unique(labor_market_label_options$key)) != length(names(poster_res))) return(NULL)

			if (!all(sort(unique(labor_market_label_options$key)) == sort(unique(names(poster_res))))) return(NULL)

			if (!all(map(poster_res, \(k) length(k)) == 2)) return(NULL)

			list_rbind(imap(poster_res, \(k, name) tibble(key = name, value = k[[1]], rationale = k[[2]]))) %>%
				bind_cols(scrape_id = scrape_ids[[j]], post_id = post_ids[[j]], .)

			})))

		return(group_gpt_results)
	}))

	# Remove any post_ids where *any* of the label keys or values are invalid
	filtered_res =
		res %>%
		left_join(
			.,
			transmute(labor_market_label_options, key, value, label_exists = 1),
			by = c('key', 'value')
			) %>%
		mutate(., is_na = ifelse(is.na(label_exists), 1, 0)) %>%
		mutate(., post_invalid_keys_or_values = sum(is_na), .by = post_id) %>%
		filter(., post_invalid_keys_or_values == 0) %>%
		select(., -is_na, -post_invalid_keys_or_values) %>%
		transmute(
			.,
			scrape_id,
			post_id,
			prompt_id = labor_market_prompt_id,
			label_key = key,
			label_value = value,
			label_rationale = rationale
		)


	message('***** Unique post IDs returned: ', length(unique(filtered_res$post_id)))
	message('***** Unique post IDs desired: ', sum(map_int(user_prompts, \(x) length(x$post_ids))))

	llm_outputs$labor_market <<- filtered_res
})




# Finalize --------------------------------------------------------

## Write Scores to SQL  --------------------------------------------------------
local({

	data_to_sql = bind_rows(llm_outputs)

	message(str_glue('*** Sending GPT Data to SQL: {format(now(), "%H:%M")}'))

	initial_count = get_rowcount(pg, 'text_scraper_reddit_llm_scores_v2')
	message('***** Initial Count: ', initial_count)

	insert_groups =
		data_to_sql %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_split(., split, .keep = F)

	insert_result = map_dbl(insert_groups, .progress = F, function(x)
		dbExecute(pg, create_insert_query(
			x,
			'text_scraper_reddit_llm_scores_v2',
			'ON CONFLICT (post_id, prompt_id, label_key) DO UPDATE SET
				scrape_id=EXCLUDED.scrape_id,
				label_value=EXCLUDED.label_value,
				label_rationale=EXCLUDED.label_rationale'
			))
	)

	insert_result = {if (any(is.null(insert_result))) stop('SQL Error!') else sum(insert_result)}

	final_count = get_rowcount(pg, 'text_scraper_reddit_llm_scores_v2')
	rows_added = final_count - initial_count
	message('***** Rows Added: ', rows_added)

	disconnect_db(pg)

	validation_log$post_label_combinations <<- rows_added
})
