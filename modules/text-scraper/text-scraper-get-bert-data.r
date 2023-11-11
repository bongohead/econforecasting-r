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

# Financial Health --------------------------------------------------------

## Constants ---------------------------------------------------------------
local({
	financial_health_prompt_id <<- 'financial_health_v1'
	financial_health_sample_size <<- 10000
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
				AND a.selftext IS NOT NULL
				AND a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
				AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) BETWEEN 10 AND 1000
				AND (
					(DATE(a.created_dttm) BETWEEN '2018-01-01' AND '2022-12-31' AND scrape_method = 'pushshift_backfill')
					OR (DATE(a.created_dttm) BETWEEN '2023-01-01' AND '2023-08-20' AND scrape_method = 'pullpush_backfill')
					OR DATE(a.created_dttm) > '2023-08-20'
				)
		)
		SELECT * FROM t0 WHERE rn = 1 ORDER BY random()
		LIMIT {financial_health_sample_size}"
	))

	samples %>% count(., date = created_dt) %>% ggplot + geom_line(aes(x = date, y = n))

	financial_health_samples <<- samples
})

## Prompts -----------------------------------------------------------------
local({
	
	keys = c(
		'id',
		'financial_sentiment','financial_sentiment_rationale',
		'employment_status','employment_status_rationale',
		'unemployment_reason','unemployment_reason_rationale',
		'employment_satisfaction','employment_satisfaction_rationale'
	)
	
	base_prompts = list(
		system =
			"You will be given a list of posts made by users on the career/finance board of a social media website.
			Please identify whether each user is financially doing well or struggling, based off the content of their post.

			Return a JSON containing an array of objects, where each object should correspond to one post. 
			
			The object keys are as follows:
			
			1. id: The post ID provided.
			1. financial_sentiment: How financially healthy does the user feel? Choose between 'neutral', 'weak', or 'strong'. If no indication is given, choose 'neutral'. Use both the content of the user's post as well as the mood of the post to make this decision. For example, things that could indicate 'strong' could be a pay raise, saving a large amount of money for retirement, or a general celebratory mood.
			2. financial_sentiment_rationale: An explanation of the choice for financial_sentiment.
			3. employment_status: Is the user currently employed or unemployed? Choose between 'unemployed', 'employed', or 'unknown'.
			4. employment_status_rationale: An explanation of the choice for employment_status.
			5. unemployment_reason: If employment_status == 'unemployed', what was the cause? Choose between 'fired_or_laid_off', 'never_employed', 'resigned', or 'unknown'. Set to null IF AND ONLY IF employment_status != 'unemployed'.
			6. unemployment_reason_rationale: An explanation of the choice for unemployment_reason. Set to null if and only if employment_status != 'unemployed'.
			7. employment_satisfaction: If employment status == 'employed', how does the user feel about their current job? Choose between 'satisfied', 'unsatisfied', or 'unknown'. Set to null IF AND ONLY IF employment_status != 'employed'.
			8. employment_satisfaction_rationale: An explanation of the choice for employment_satisfaction. Set to null if and only if employment_status != 'employed'.
			
			Think carefully, step by step, and try your best to provide accurate explanations.",
		user =
			"*ID: 1*
			TITLE: Word Of Warning
			POST: So this post is not to garner sympathy, I've learned from my mistake just hoping others can learn as well. I was in a bad place emotionally and financially, I took a Payday loan for 150 dollars, now I could have paid it off from my first paycheck but I didn't let payments get made instead. But then I checked my loan account and I was paying 500 dollars on a 150 dollar loan. I lost my job couldn't pay the rest off so just let the payments come.

			*ID: 2*
			TITLE: I was recently given $14k. Pay off my car or invest?
			POST: I make 74k a year before taxes I had 18k in my checking account. I recently inherited 14k from a generous relative passing away, bringing me to 32k. I have 16k left on my car at something like 2.8 API. This is my only debt. Should I put that money into stocks? or just pay off my car loan?

			*ID: 3*
			TITLE: Looking for an app to track my funds and budget
			POST: Hello, I am looking for an easy to use app to track all my funds and budget. I do not want to link any of my bank or investment accounts to it and am okay with entering them in manually.

			*ID: 4*
			TITLE: [US] I've been living paycheck-to-paycheck all of my life. How can I stop living like this?
			POST: Let me provide some background on me:  I earn $65k/yr & I of course have bills including a lot of credit card debt. The way I've always functioned is I split my bills in 1/2 to pay them when I get paid; then whatever's leftover is spending $ for the week. I do have difficulty saving & I was told it's probably due to my ADHD. I have no idea how to stop this living from paycheck-to-paycheck life &amp; was looking for advice on how to do so.

			*ID: 5*
			TITLE: I basically went to college for nothing - Unemployed and Depressed.
			POST: I got a Bachelors in Marketing a few years ago. I didn’t really take full advantage of being in school and preparing for the real world. Since graduating, I’ve submitted over 1300 applications to white collar jobs with multiple iterations of a resume. I usually apply to Marketing Coordinator roles or anything entry-level. At this point, I’m at a loss.

			*ID: 6*
			TITLE: Started a new job and a company I applied to before just reached out and gave me an offer for 3 times my salary
			POST: I'm underpaid at my current job. I like my boss and team, but I probably won't ever come back to the company. How should I approach leaving? I don't start the new job for another month, and I've accepted an offer at my new job. Do I tell my boss and team now?

			*ID: 7*
			TITLE: Would I be absolutely stupid to quit my high paying job?
			POST: I have a well paying job (low six figures) and I’ve been at my company almost a decade, but I'm burnt out from the politics. Why am I so terrified to quit? I feel like I’ve put so much time and energy into climbing high in the pay scale and wherever I end up is going to be just as bad

			*ID: 8*
			TITLE: Anyone else ever been in this predicament?
			POST: I am poor. I mean dead broke. I got a job at Wendy's today and I start a week from Monday. She said I had to have black pants and black no skid shoes. I didn’t tell her but there’s no way I can afford to get those by Monday. I’m living off or ramen right now. Should I just show up Monday in some dark blue pants?"
		,
		assistant = list(posts = list(
			list(
				id = 1,
				financial_sentiment = 'weak', financial_sentiment_rationale = 'The user lost their job and "took a Payday loan".',
				employment_status = 'unemployed', employment_status_rationale = 'The user states they "lost" their job, showing they\'re unemployed.',
				unemployment_reason = 'fired_or_laid_off', unemployment_reason_rationale = 'The user states they "lost" their job, suggesting they lost their job involuntarily.',
				employment_satisfaction = NA, employment_satisfaction_rationale = NA
				),
			list(
				id = 2,
				financial_sentiment = 'strong', financial_sentiment_rationale = 'The user inherited some money from a "generous relative".',
				employment_status = 'employed', employment_status_rationale = 'The user states they "make 74k a year", suggesting they have a job.',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'unknown', employment_satisfaction_rationale = 'The user doesn\'t give any indication about how they feel about their current job.'
			),
			list(
				id = 3,
				financial_sentiment = 'neutral', financial_sentiment_rationale = 'The user gives no indication of their financial health.',
				employment_status = 'unknown', employment_status_rationale = 'The user does not indicate whether they have a job.',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = NA, employment_satisfaction_rationale = NA
			),
			list(
				id = 4,
				financial_sentiment = 'weak', financial_sentiment_rationale = 'The user says they\'re living "paycheck-to-paycheck" and have a lot of credit card debt.',
				employment_status = 'employed', employment_status_rationale = 'The user says they earn 65k a year, indicating they have a job.',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'unknown', employment_satisfaction_rationale = 'The user doesn\'t mention how they feel about their current job.'
			),
			list(
				id = 5,
				financial_sentiment = 'weak', financial_sentiment_rationale = 'The user is "unemployed and depressed".',
				employment_status = 'unemployed', employment_status_rationale = 'The user is "unemployed and depressed".',
				unemployment_reason = 'never_employed', unemployment_reason_rationale = 'The user seems to be searching for their first job after graduating.',
				employment_satisfaction = NA, employment_satisfaction_rationale = NA
			),
			list(
				id = 6,
				financial_sentiment = 'strong', financial_sentiment_rationale = 'The user recently received a large pay raise by switching to a higher-paying job."',
				employment_status = 'employed', employment_status_rationale = 'The user has a current job as well as another job they\'re switching to.',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'unsatisfied', employment_satisfaction_rationale = 'The user states they\'re "underpaid" at their current job.'
			),
			list(
				id = 7,
				financial_sentiment = 'strong', financial_sentiment_rationale = 'The user mentions they have a "well paying" job".',
				employment_status = 'employed', employment_status_rationale = 'The user states they\'re currently employed, though they don\'t like their job.',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'unsatisfied', employment_satisfaction_rationale = 'The user mentions being "burnt out from the politics" at their current job.'
			),
			list(
				id = 8,
				financial_sentiment = 'weak', financial_sentiment_rationale = 'The user mentions they are "dead broke".',
				employment_status = 'employed', employment_status_rationale = 'The user states they "got a job at Wendy\'s".',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'unknown', employment_satisfaction_rationale = 'The user doesn\'t indicate any particular sentiment about their new job.'
			)
			)),
		user =
			"*ID: 1*
			TITLE: Advice Wanted, Can I Afford 1200/month Rent?
			POST: I make $21 an hour and am wondering if I could afford $1200/month rent. Found a great large studio with a reverse commute and will let me bring my dog, and I have scheduled a tour and am considering in applying for the place. My monthly income is about $2500 after taxes (this is my 90 day average income), and my current expenses are around $650 a month.

			*ID: 2*
			TITLE: How do I find a career if my degree is vague?
			POST: Which job boards are the best? What are some free online resume resources? I was fired last week (completely bs, incompetent management) I can get unemployment but I would rather find a job ASAP. Its always really hard for me bc i have a weird degree (psych and cj, wanted to be a cop not anymore) degree and no hard technical skills to market.

			*ID: 3*
			TITLE: Career change after 20 years; impossible?
			POST: I work as a manager at a convenient store. While I like it and it's good money, I want a change. Are there any programs or resources I should be looking into that can help me?
		
			*ID: 4*
			TITLE: 52 and Have No Retirement. NONE
			POST: I have worked as a veterinary technician (we don't make much). I have a master's degree and loans and about 20K in credit card debt. I secured a really nice paying job for the first time in my life and have about 10k in my bank account. I am scared to do anything with that money. As someone who had to live check to check, investing or paying off my cards seeing a low balance again gives me anxiety. I know I should do this but I just don't know where to begin. Help!",
		assistant = list(posts = list(
			list(
				id = 1,
				financial_sentiment = 'neutral', financial_sentiment_rationale = 'The user is neutral about their financial well-being.',
				employment_status = 'employed', employment_status_rationale = 'The user states they make "$21 an hour", directly showing they have a job.',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'unknown', employment_satisfaction_rationale = 'The user doesn\'t demonstrate any specific feelings about their job.'
			),
			list(
				id = 2,
				financial_sentiment = 'weak', financial_sentiment_rationale = 'The user was "fired last week" and is having difficulty finding a new job.',
				employment_status = 'unemployed', employment_status_rationale = 'The user is unemployed and searching for a job.',
				unemployment_reason = 'fired_or_laid_off', unemployment_reason_rationale = 'The user states they were "fired last week".',
				employment_satisfaction = NA, employment_satisfaction_rationale = NA
			),
			list(
				id = 3,
				financial_sentiment = 'neutral', financial_sentiment_rationale = 'The user is relatively neutral about their financial well-being.',
				employment_status = 'employed', employment_status_rationale = 'The user works "as a manager at a convenience store".',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'satisfied', employment_satisfaction_rationale = 'The user states they "like" their job and that it pays "good money".'
			),
			list(
				id = 4,
				financial_sentiment = 'weak', financial_sentiment_rationale = 'The user is stressed that they "have no retirement" at 52.',
				employment_status = 'employed', employment_status_rationale = 'The user "secured a really nice paying job".',
				unemployment_reason = NA, unemployment_reason_rationale = NA,
				employment_satisfaction = 'satisfied', employment_satisfaction_rationale = 'The user states they have a "nice paying job".'
			)
		))
		) %>%
		imap(., \(x, i) list(
			role = i,
			content = {
				if (i == 'system') str_replace_all(x, '\\t', '')
				else if (i == 'user') str_replace_all(x, '\\t', '')
				else if (i == 'assistant') toJSON(x, auto_unbox = T)
			}
		)) %>% unname(.)

	input_data =
		financial_health_samples %>%
		mutate(., score_group = ceiling((1:nrow(.))/20)) %>%
		mutate(., user_message = paste0(
			'*ID: ', 1:n(), 
			'*\nTITLE: ', title,
			'\nPOST: ', str_squish(str_replace_all(selftext, "\\t|\\n", " "))
			), .by = score_group) %>%
		group_split(., score_group) %>%
		imap(., \(x, i) list(
			score_group = unique(x$score_group),
			scrape_ids = x$scrape_id,
			post_ids = x$post_id,
			user_message = paste0(x$user_message, collapse = '\n\n'),
			user_messages = x$user_message,
			source_boards = x$source_board,
			scrape_methods = x$scrape_method
		))

	input_requests =
		map(input_data, \(p) p$user_message)  %>%
		map(., \(m) {
			request('https://api.openai.com/v1/chat/completions') %>%
			req_headers(
				'Authorization' = paste0('Bearer ', Sys.getenv('OPENAI_API_KEY')),
				'Content-Type' = 'application/json'
			) %>%
			req_body_raw(str_glue(
			'{{
				"model": "gpt-3.5-turbo-1106",
				"messages": {messages_list},
				"response_format": {{"type": "json_object"}},
				"temperature": 0.2
			}}',
			messages_list = toJSON(
				c(base_prompts, list(list(role = 'user', content = m))),
				auto_unbox = T
				)
			)) %>%
			req_timeout(60 * 3)
		})

	clean_responses = function(data_with_responses, .echo = T) {
		
		list_rbind(map(data_with_responses, function(r) {

			response_json = resp_body_json(r$response)
			response_content = response_json$choices[[1]]$message$content

			# Throw non-JSON outputs
			if (validate(response_content) == F) return(NULL)
			json_parsed = fromJSON(response_content, simplifyVector = F)
	
			# Throw out incorrect counts, since uncertain which one corresponds to what
			if (!'posts' %in% names(json_parsed)) return(NULL)
			if (length(json_parsed$posts) != length(r$post_ids)) return(NULL)
	
			# Iterate through each and validate keys, result types
			group_gpt_results = list_rbind(compact(imap(json_parsed$posts, function(poster_res, j) {
	
				if (length(unique(keys)) != length(names(poster_res))) return(NULL)
				if (!all(sort(unique(keys)) == sort(unique(names(poster_res))))) return(NULL)
	
				post_no_key = poster_res[names(poster_res) != 'id']
				
				if (!all(map_lgl(post_no_key, \(v) is_scalar_character(v) | is_null(v)))) return(NULL)
				if (!poster_res['id'] %in% 1:length(r$post_ids)) return(NULL)
				query_id = as.integer(poster_res['id'])
				
				imap(post_no_key, \(v, name) tibble(key = name, value = coalesce(v, NA_character_))) %>%
					list_rbind() %>%
					bind_cols(scrape_id = r$scrape_ids[[query_id]], post_id = r$post_ids[[query_id]], query_id = query_id, .)
			})))
			
			if (nrow(group_gpt_results) > 0 && .echo == T) {
				message('Total token length: ', response_json$usage$total_tokens)
				sample_id = sample(1:length(r$post_ids), size = 1)
				message('\n\nSample: ', r$source_boards[sample_id], ' | ', r$scrape_methods[sample_id])
				cat(r$user_messages[sample_id])
				message('\n------\n')
				print(select(filter(group_gpt_results, post_id == r$post_ids[sample_id]), query_id, key, value))
			}
		
			return(group_gpt_results)
		}))
		
	}
	
	# Send double requests
	multi_res = map(1:2, \(s) {
		http_responses = send_async_requests(input_requests, .chunk_size = 20, .max_retries = 5, .verbose = T)
		data_with_responses = imap(http_responses, \(r, i) c(input_data[[i]], list(response = r)))
		clean_responses(data_with_responses) %>% mutate(., s = s)
	})
	
	# Retain post IDs where both responses are identical across all key/values
	accurate_post_ids =
		multi_res %>%
		bind_rows(.) %>%
		pivot_wider(
			.,
			id_cols = c(scrape_id, post_id, query_id, key), 
			names_from = s, 
			values_from = value,
			names_prefix = 'value_'
			) %>%
		filter(., !str_detect(key, '_rationale')) %>%
		mutate(., is_mismatch = case_when(
			is.na(value_1) & is.na(value_2) ~ 0,
			is.na(value_1) & !is.na(value_2) ~ 1,
			!is.na(value_1) & is.na(value_2) ~ 1,
			value_1 == value_2 ~ 0,
			TRUE ~ 1
		)) %>%
		group_by(., post_id) %>%
		summarize(., mismatch = sum(is_mismatch), .groups = 'drop') %>%
		filter(., mismatch == 0) %>% 
		.$post_id

	filtered_res = filter(multi_res[[1]], post_id %in% accurate_post_ids)
	
	final_res =
		bind_rows(
				mutate(filter(filtered_res, str_detect(key, '_rationale') == T), type = 'rationale'),
				mutate(filter(filtered_res, str_detect(key, '_rationale') == F), type = 'value')
		) %>%
		mutate(., key = str_replace_all(key, '_rationale|_value', '')) %>%
		pivot_wider(., id_cols = c(scrape_id, post_id, key), names_from = type, values_from = value) %>%
		transmute(
			.,
			scrape_id,
			post_id,
			prompt_id = financial_health_prompt_id,
			label_key = key,
			label_value = value,
			label_rationale = rationale
		)
	
	message('***** Unique post IDs returned: ', length(unique(final_res$post_id)))
	message('***** Unique post IDs desired: ', sum(map_int(input_data, \(x) length(x$post_ids))))

	llm_outputs$financial_health <<- final_res
})

# Personal Finance ------------------------------------------------------

## Constants ---------------------------------------------------------------
# local({
# 	financial_health_prompt_id <<- 'financial_health_v1'
# 	financial_health_sample_size <<- 1000000
# })

## Pull Samples  --------------------------------------------------------
# local({
# 
# 	# Only score each post from the source table once, even if duplicated in source table (across methods)
# 	# Anti join with existing scores on post_id to prevent re-scoring the same post_ids
# 	samples = get_query(pg, str_glue(
# 		"WITH t0 AS (
# 			SELECT
# 				a.scrape_id, a.post_id,
# 				a.source_board, a.scrape_method,
# 				a.title, a.selftext,
# 				DATE(a.created_dttm) AS created_dt,
# 				ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
# 				ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
# 			FROM text_scraper_reddit_scrapes a
# 			LEFT JOIN text_scraper_reddit_llm_scores_v2 b
# 				ON a.post_id = b.post_id
# 				AND b.prompt_id = '{financial_health_prompt_id}'
# 			WHERE
# 				b.score_id IS NULL
# 				AND a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
# 				AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) BETWEEN 10 AND 1000
# 		)
# 		SELECT * FROM t0 WHERE rn = 1 ORDER BY random() 
# 		LIMIT {financial_health_sample_size}"
# 	))
# 
# 	samples %>% count(., date = created_dt) %>% ggplot + geom_line(aes(x = date, y = n))
# 	
# 	regex_res = 
# 		samples %>%
# 		mutate(
# 			., 
# 			text = paste0('TITLE: ', title, '\nPOST: ', str_replace_all(selftext, "\\t|\\n", " ")),
# 			is_layoff = ifelse(str_detect(text, 'layoff|laid off|fired|unemployed|lost( my|) job|laying off'), 1, 0),
# 			is_resign = ifelse(str_detect(text, 'quit|resign|weeks notice|(leave|leaving)( a| my|)( job)'), 1, 0),
# 			is_new_job = ifelse(str_detect(text, 'hired|new job|background check|job offer|(found|got|landed|accepted|starting)( the| a|)( new|)( job| offer)'), 1, 0),
# 			is_searching = ifelse(str_detect(text, 'job search|application|applying|rejected|interview|hunting'), 1, 0),
# 			is_inflation = ifelse(str_detect(text, 'inflation|cost-of-living|cost of living|expensive'), 1, 0)
# 			# is_housing = ifelse(str_detect(text, 'housing|home|rent'), 1, 0)
# 			) %>%
# 		group_by(., created_dt) %>%
# 		summarize(
# 			., 
# 			across(starts_with('is_'), sum),
# 			count = n(),
# 			.groups = 'drop'
# 			) %>%
# 		arrange(., created_dt)
# 	
# 	regex_res %>%
# 		mutate(across(c(starts_with('is_'), count), function(x)
# 			zoo::rollapply(
# 				x, width = 180,
# 				FUN = function(x) sum(.98^((length(x) - 1):0) * x),
# 				fill = NA, align = 'right'
# 				)
# 			)) %>%
# 		pivot_longer(., -c(created_dt, count), names_to = 'name', values_to = 'value', values_drop_na = T) %>%
# 		mutate(., ratio = value/count) %>%
# 		ggplot() +
# 		geom_line(aes(x = created_dt, y = ratio, color = name, group = name))
# 
# 	financial_health_samples <<- samples
# })

## Prompts -----------------------------------------------------------------
# local({
# 	
# # 	custom_function = str_replace_all('[
# #   {
# # 		"name": "classify_posts",
# #     "description": "Classifies content and sentiment of a variety of society media posts for a personal finance forum",
# #     "parameters": {
# #       "type": "object",
# #       "properties": {
# #         "posts": {
# #           "type": "array",
# #           "description": "An array of subarrays where each subarray represents a post",
# #           "items": {
# #           	"type": "array",
# #           	"description": "An array of objects where each object reperesents a possible category for the post",
# #           	"items": {
# # 	          	"type": "object",
# # 	          	"properties": {
# # 	          		"label": {
# # 	          			"type": "string",
# # 	          			"description": "A label which describes the content of the post. Multiple labels can be assigned per post. 
# # 	          			"enum": ["financially_distressed", "financially_strong", "income_increase", "struggling_with_debt", "recently_terminated", "recently_resigned", "inflation", "housing", "salary"]
# # 	          		},
# # 	          		"rationale": {
# # 	          			"type": "string",
# # 	          			"description": "A brief explanation for why the post justifies the label"
# # 	          		}
# # 	          	},
# # 		          "required": ["label", "rationale"]
# #           	}
# #           }
# #         }
# #       },
# #       "required": ["posts"]
# #     }
# #   }]', '\\n|\\t', '')
# 	
# 	# keys = names(fromJSON(custom_function, simplifyVector = F)[[1]]$parameters$properties$posts$items)
# 	
# 	base_prompts = list(
# 		system = 
# 			"You will be given a list of posts made by users on the career/finance board of a social media website. Please label the content of these posts.
# 			
# 			Return a JSON containing an array of subarrays. Each subarray should correspond to a single post. 
# 			Each subarray should contain objects where each object contains a label describing the content of the post, and a rationale for that label.
# 			
# 			For example, if the post mentions the user's credit card debt, you can return {label: \"struggling_with_debt\", rationale: \"The post is about the user's credit card debt\"}.
# 			For a single post, you can return no label, one label, or multiple labels when needed.
# 			
# 			Please ONLY return labels from a predefined list of 11 labels. The labels and guidance for when to apply each label are provided below.
# 
# 			1. is_financially_distressed: The post indicates the user or someone they know is financially stressed (e.g. struggling with debt or bills, bankruptcy)
# 			2. is_financially_strong: The post indicates the user or someone they know is feeling financially healthy (e.g. pay raise, received gift)
# 			3. received_income_increase: The post indicates the user or someone they know recently received an increase in income (e.g. pay raise, higher-paying job)
# 			4. is_struggling_with_debt: The post indicates the user or someone they know is struggling with their debt load (e.g. credit cards, mortgages)
# 			5. recently_terminated: The post indicates the user or someone they know recently was laid off or fired
# 			6. recently_resigned: The post indicates the user or someone they know recently left a job voluntarily (not including retirement)
# 			7. currently_unemployed: The post indicates the user or someone they know is currently_unemployed
# 			8. unhappy_at_current_job: The post indicates the user or someone they know is unhappy with their CURRENT job for reasons such as pay, poor work conditions, etc
# 			9. received_job_offer: The post indicates the user or someone they know received a new job offer.
# 			10. inflation: The post indicates the user or someone they know is thinking about or is concerned with inflation or cost of living
# 
# 			Many times, none of these labels will be relevant! In that case, simply return an empty array for the post.
# 
# 			Think carefully and slowly. Accuracy is the MOST important thing here.",
# 		user = 
# 			"*POST #1*
# 			TITLE: Word Of Warning
# 			POST: So this post is not to garner sympathy, I've learned from my mistake just hoping others can learn as well. I was in a bad place emotionally and financially, I took a Payday loan for 150 dollars, now I could have paid it off from my first paycheck but I didn't I let payments get made instead. But then I checked my loan account and I was paying 500 dollars on a 150 dollar loan. I lost my job couldn't pay the rest off so just let the payments come.
# 			
# 			*POST #2*
# 			TITLE: I was recently given $14k. Pay off my car or invest?
# 			POST: I make 74k a year before taxes I had 18k in my checking account. I recently inherited 14k from a generous relative passing away, bringing me to 32k. I have 16k left on my car at something like 2.8 API. This is my only debt. Should I put that money into stocks or some kind of investing account? or just pay off my car loan?
# 			
# 			*POST #3*
# 			TITLE: Looking for an app to track my funds and budget
# 			POST: Hello, I am looking for an easy to use app to track all my funds and budget. I do not want to link any of my bank or investment accounts to it and am okay with entering them in manually. 
# 			
# 			*POST #4*
# 			TITLE: [US] I've been living paycheck-to-paycheck all of my life. How can I stop living like this?
# 			POST: Let me provide some background on me:  I earn $65k/yr &amp; I of course have bills including a lot of credit card debt. The way I've always functioned is I split my bills in 1/2 to pay them when I get paid &amp; then whatever's leftover is spending $ for the week. I do have difficulty saving &amp; I was told it's probably due to my ADHD. I have no idea how to stop this living from paycheck-to-paycheck life &amp; was looking for advice on how to do so.
# 			
# 			*POST #5*
# 			TITLE: I basically went to college for nothing - Unemployed and Depressed.
# 			POST: I got a Bachelors in Business Administration in Marketing a few years ago. I didn’t really take full advantage of being in school and preparing for the real world. Since graduating, I’ve submitted over 1300 applications to white collar jobs with multiple iterations of a resume, and have gotten only one offer at McDonald's but got fired. I usually apply to Marketing Coordinator roles or anything entry-level. At this point, I’m at a loss.
# 		
# 			*POST #6*
# 			TITLE: Started a new job and a company I applied to before just reached out and gave me an offer for 3 times my salary
# 			POST: My current job is fine, but I'm underpaid. I like my boss and team, but I probably won't ever come back to the company. How should I approach leaving? I don't start the new job for another month, and I've accepted an offer at my new job. Do I tell my boss and team now?
# 
# 			*POST #7*
# 			TITLE: Would I be absolutely stupid to quit my high paying job?
# 			POST: I have a well paying job (low six figures) and I’ve been at my company almost a decade, but I'm burnt out from the politics. Why am I so terrified to quit? I feel like I’ve put so much time and energy into climbing high in the pay scale and wherever I end up is going to be just as bad
# 			
# 			*POST #8*
# 			TITLE: Anyone else ever been in this predicament?
# 			POST: I am poor. I mean I am dead broke. I got a job at Wendy’s today and I start a week from Monday. She said I had to have black pants and black no skid shoes. I didn’t tell her but there’s no way I can afford to get those by Monday. I’m living off or ramen right now. Should I just show up Monday in some dark blue pants? 
# 			
# 			*POST #9*
# 			TITLE: I'm 63 years old and looking for housing support.
# 			POST: I've had low income for last 10 years and no house ever since I sold it 10 years ago and gifted that money to my son. I don't have a lot of savings. Am I eligible for Senior housing program? Who should I  contact?
# 			
# 			*POST #10*
# 			TITLE: How do capital gains taxes work?
# 			POST: If I buy a stock for $100 and sell it for $150, am I being taxed for all $150 of that stock or only the $50 that im profiting from?",
# 		assistant = list(
# 			'1' = list( #1
# 				list(label = 'is_financially_distressed', rationale = 'The user is struggling to make payments on their loan.'),
# 				list(label = 'is_struggling_with_debt', rationale = 'The user "took a Payday loan" out of desperation.'),
# 				list(label = 'recently_terminated', rationale = 'The user mentions "I lost my job".'),
# 				list(label = 'currently_unemployed', rationale = 'The user mentions they lost their job.')
# 				),
# 			'2' = list( #2
# 				list(label = 'is_financially_strong', rationale = 'The user recently came into a lot of money ("I recently inherited 14k").')
# 				),
# 			'3' = list( #3
# 				),
# 			'4' = list( #4
# 				list(label = 'is_financially_distressed', rationale = 'The user mentions living "paycheck-to-paycheck".'),
# 				list(label = 'is_struggling_with_debt', rationale = 'The user says they have "a lot of credit card debt".')
# 				),
# 			'5' = list( #5
# 				list(label = 'is_financially_distressed', rationale = 'The user mentions they are "unemployed and depressed".'),
# 				list(label = 'recently_terminated', rationale = 'The user mentions they "got fired" at McDonald\'s.'),
# 				list(label = 'currently_unemployed', rationale = 'The user is currently "unemployed and depressed".')
# 			),
# 			'6' = list( #6
# 				list(label = 'is_financially_strong', rationale = 'The user recently received a large pay raise by switching to a higher-paying job.'),
# 				list(label = 'received_income_increase', rationale = 'The user received "an offer for 3 times my salary".'),
# 				list(label = 'recently_resigned', rationale = 'The user is resigning from their current job and is asking for advice on how to "approach leaving".'),
# 				list(label = 'unhappy_at_current_job', rationale = 'The user mentions they are "underpaid" at their current job.'),
# 				list(label = 'received_job_offer', rationale = 'The user recently received "an offer" for a new job.')
# 			),
# 			'7' = list( #7
# 				list(label = 'is_financially_strong', rationale = 'The user mentions they have a "well paying job".'),
# 				list(label = 'unhappy_at_current_job', rationale = 'The user is unhappy due to the "politics" at their job.')
# 				),
# 			'8' = list( #8
# 				list(label = 'is_financially_distressed', rationale = 'The user mentions they are "dead broke".'),
# 				list(label = 'received_income_increase', rationale = 'The user got a job, but seems previously to have been unemployed.'),
# 				list(label = 'received_job_offer', rationale = 'The user "got a job at Wendy\'s.')
# 			),
# 			'9' = list( #9
# 				list(label = 'is_financially_distressed', rationale = 'The user mentions they are "low income".')
# 			),
# 			'10' = list( #10
# 				)
# 			),
# 		user = 
# 			"*POST #1*
# 			TITLE: Advice Wanted, Can I Afford 1200/month Rent?
# 			POST: I make $21 an hour and am wondering if I could afford $1200/month rent. Found a great large studio with a reverse commute and will let me bring my dog, and I have scheduled a tour and am considering in applying for the place.  My monthly income is about $2500 after taxes (this is my 90 day average income right now), and my current expenses are around $650 a month
# 		
# 			*POST #2*
# 			TITLE: Salary for work-from-home positions: why do employers get to pay an employee less for doing the same work, just because the employee lives in a city with lower cost of living?
# 			POST: Related: one of the benefits of wfh jobs is you can do them from anywhere. But if you accept a job where the salary is based on your current location, doesn’t that tie you to that location? My city is crazy expensive these days.
# 			
# 			*POST #3*
# 			TITLE: Two weeks into work and I’m dreading it everyday.
# 			POST: I’ve just started working full time at a tech company. I came from a graphic design background and recently change my path to be a UI/UX designer for a pay increase. I am starting to find myself dreading to go to work every morning and feeling all kind of down.
# 			
# 			*POST #4*
# 			TITLE: How do I find a career if my degree is vague?
# 			POST:  Which job boards are the best? What are some free online resume resources? I was fired last week (completely bs, incompetent management) I can get unemployment but I would rather find a job ASAP. Its always really hard for me bc i have a weird degree (psych and cj, wanted to be a cop not anymore) degree and no hard technical skills to market.
# 		
# 			*POST #5*
# 			TITLE: How long before it's is too long to accept an offer
# 			POST: Let's just say I get an offer on Friday. But am having phone interview and in person with another company through mid week the following week. Is it acceptable not to accept for a week or let them know by end of the week?
# 			
# 			*POST #6*
# 			TITLE: Career change after 20 years; impossible?
# 			POST: I work as a manager at a convenient store. While I like it and it's good money, the winds of change are blowing the place towards a port I don't want to be involved in any more. Are there any programs or resources I should be looking into that can help me?
# 		",
# 		assistant = list(
# 			'1' = list(),
# 			'2' = list(
# 				list(label = 'inflation', rationale = 'The user asks about pay and mentions the high cost-of-living in their area.')
# 			),
# 			'3' = list(
# 				list(label = 'is_financially_strong', rationale = 'The user recently received a "pay increase" by becoming a UI/UX designer.'),
# 				list(label = 'received_income_increase', rationale = 'The user recently received a "pay increase" by becoming a UI/UX designer.'),
# 				list(label = 'unhappy_at_current_job', rationale = 'The user is depressed and unhappy at their new job.')
# 			),
# 			'4' = list(
# 				list(label = 'is_financially_distressed', rationale = 'The user recently lost their job.'),
# 				list(label = 'recently_terminated', rationale = 'The user was "fired last week".'),
# 				list(label = 'currently_unemployed', rationale = 'The user is currently unemployed due to their firing.')
# 			),
# 			'5' = list(
# 				list(label = 'received_job_offer', rationale = 'The user seems to have received a job offer, based off the fact that they\'re contemplating delaying accepting it.')
# 			),
# 			'6' = list(
# 				list(label = 'is_financially_strong', rationale = 'The user mentions their current job is "good money".')
# 			)
# 		)
# 		) %>%
# 		imap(., \(x, i) list(
# 			role = i,
# 			content = {
# 				if (i != 'assistant') str_replace_all(x, '\\t', '')
# 				else toJSON(x, auto_unbox = T)
# 			}
# 		)) %>% unname(.)
# 
# 	user_prompts =
# 		samples %>%
# 		sample_n(., 20) %>%
# 		mutate(., user_message = paste0(
# 			'*POST #', 1:nrow(.), '*\nTITLE: ', title, '\nPOST: ', str_replace_all(selftext, "\\t|\\n", " ")
# 			)) %>%
# 		mutate(., score_group = 1) %>%
# 		group_split(., score_group) %>%
# 		imap(., \(x, i) list(
# 			score_group = unique(x$score_group),
# 			scrape_ids = x$scrape_id,
# 			post_ids = x$post_id,
# 			user_message = paste0(x$user_message, collapse = '\n\n')
# 		))
# 	
# 	requests = map(user_prompts, \(p) {
# 		request('https://api.openai.com/v1/chat/completions') %>%
# 			req_headers(
# 				'Authorization' = paste0('Bearer ', Sys.getenv('OPENAI_API_KEY')),
# 				'Content-Type' = 'application/json'
# 			) %>%
# 			req_body_raw(str_glue(
# 			'{{
# 				"model": "gpt-4-1106-preview",
# 				"messages": {messages_list},
# 				"temperature": 0.2
# 			}}',
# 			messages_list = toJSON(
# 				c(base_prompts, list(list(role = 'user', content = p$user_message))),
# 				auto_unbox = T
# 				), 
# 			functions = custom_function
# 			)) %>%
# 			req_timeout(60 * 5)
# 	})
# 	
	# requests[[1]] %>% req_perform() -> z
# 	http_responses = send_async_requests(requests, .chunk_size = 40, .max_retries = 4, .verbose = T)
# 	
# 	res = list_rbind(imap(http_responses, function(r, i) {
# 		
# 		response_content = resp_body_json(r)$choices[[1]]$message$content
# 		
# 		# Throw non-JSON outputs
# 		if (validate(response_content) == F) return(NULL)
# 		
# 		post_ids = user_prompts[[i]]$post_ids
# 		scrape_ids = user_prompts[[i]]$scrape_ids
# 		json_parsed = fromJSON(response_content, simplifyVector = F)
# 		
# 		# Throw out incorrect counts, since uncertain which one corresponds to what
# 		if (length(json_parsed) != length(post_ids)) return(NULL)
# 		
# 		# Iterate through each and check keys, pivot longer
# 		group_gpt_results = list_rbind(compact(imap(json_parsed, function(poster_res, j) {
# 			
# 			if (length(unique(labor_market_label_options$key)) != length(names(poster_res))) return(NULL)
# 			
# 			if (!all(sort(unique(labor_market_label_options$key)) == sort(unique(names(poster_res))))) return(NULL)
# 			
# 			if (!all(map(poster_res, \(k) length(k)) == 2)) return(NULL)
# 			
# 			list_rbind(imap(poster_res, \(k, name) tibble(key = name, value = k[[1]], rationale = k[[2]]))) %>%
# 				bind_cols(scrape_id = scrape_ids[[j]], post_id = post_ids[[j]], .)
# 			
# 		})))
# 		
# 		return(group_gpt_results)
# 	}))
# 	
# })


# Labor Market Data --------------------------------------------------------

## Constants ---------------------------------------------------------------
# local({
# 	labor_market_prompt_id <<- 'labor_market_v1'
# 	labor_market_sample_size <<- 2000
# })

## Pull Samples  --------------------------------------------------------
# local({
# 
# 	# Only score each post from the source table once, even if duplicated in source table (across methods)
# 	# Anti join with existing scores on post_id to prevent re-scoring the same post_ids
# 	samples = get_query(pg, str_glue(
# 		"WITH t0 AS (
# 			SELECT
# 				a.scrape_id, a.post_id,
# 				a.title, a.selftext,
# 				a.created_dttm,
# 				ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) * 1.5 + 100 AS n_tokens,
# 				ROW_NUMBER() OVER (PARTITION BY a.post_id ORDER BY a.created_dttm DESC) AS rn
# 			FROM text_scraper_reddit_scrapes a
# 			LEFT JOIN text_scraper_reddit_llm_scores_v2 b
# 				ON a.post_id = b.post_id
# 				AND b.prompt_id = '{labor_market_prompt_id}'
# 			WHERE
# 				b.score_id IS NULL
# 				AND a.source_board IN ('jobs', 'careerguidance', 'personalfinance')
# 				AND a.selftext IS NOT NULL
# 				AND a.ups >= 10
# 				AND a.scrape_method IN (
# 					'pushshift_backfill', 'top_200_today', 'top_1000_today',
# 					'top_1000_week', 'top_1000_month', 'top_1000_year'
# 					)
# 				AND a.selftext NOT IN ('', '[deleted]', '[removed]')
# 				AND ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(a.selftext), '\\s+'), 1) <= 1000
# 		)
# 		SELECT * FROM t0 WHERE rn = 1 ORDER BY random() LIMIT {labor_market_sample_size}"
# 	))
# 
# 	samples %>% count(., date = date(created_dttm)) %>% ggplot + geom_line(aes(x = date, y = n))
# 
# 	labor_market_samples <<- samples
# })


## Prompts -----------------------------------------------------------------
# local({
# 
# 
# 	label_options = tribble(
# 		~ key, ~ value,
# 		'employment_status', 'employed',
# 		'employment_status', 'unemployed',
# 		'employment_status', 'unknown',
# 		'recently_seperated', 'resigned',
# 		'recently_seperated', 'fired/laid off',
# 		'recently_seperated', 'considering seperation',
# 		'recently_seperated', 'no/unknown',
# 		'job_search_status', 'received offer/started new job',
# 		'job_search_status', 'searching/considering search',
# 		'job_search_status', 'not searching/unknown',
# 		'recently_received_pay_increase', 'yes - significant off',
# 		'recently_received_pay_increase', 'yes - small/unknown size',
# 		'recently_received_pay_increase', 'no/unknown',
# 		'includes_pay_complaint', 'yes',
# 		'includes_pay_complaint', 'no',
# 		) %>%
# 		mutate(., key_order = consecutive_id(key))
# 
# 	opts_text =
# 		label_options %>%
# 		group_by(., key) %>%
# 		summarize(., key_order = unique(key_order), values = paste0(value, collapse = ', ')) %>%
# 		arrange(., key_order) %>%
# 		{paste0('- ', .$key, ': ', .$values)} %>%
# 		paste0(., collapse = '\n')
# 
# 	base_prompts = list(
# 		system = str_glue(
# 		"Given a list of posts made on the job advice board of a social media site, return an array of objects in JSON format.
# 	    Each object should correspond to a single post. The keys to each object and their possible values should be:
# 		{opts_text}
# 		You may ONLY use these values. Return a rationale for each value as well."
# 		),
# 		user = list(
# 			"Started a new job and a company I applied to before just reached out and gave me an offer for 3 times my salary
# 
# 			My current job is fine, but I'm underpaid. I like my boss and team, but I probably won't ever come back to the company.
# 			How should I approach leaving? I don't start the new job for another month, and I've accepted an offer at my new job.
# 			Do I tell my boss and team now?",
# 			"I basically went to college for nothing - Unemployed and Depressed.
# 
# 			I got a Bachelors in Business Administration in Marketing a few years ago.
# 			I didn’t really take full advantage of being in school and preparing for the real world. Since graduating,
# 			I’ve submitted over 1300 applications to white collar jobs with multiple iterations of a resume, and have gotten
# 			only one offer at McDonald's but got fired.
# 			I usually apply to Marketing Coordinator roles or anything entry-level. At this point, I’m at a loss.",
# 			"Can someone explain what’s going on with the market?
# 
# 			A few years ago, with less experience, I applied for a few jobs and got offers to 2 and they paid me to relocate.
# 			Now, I apply to 100 jobs to get a handful of first round interviews. Is the market really so horrible?",
# 			"Would I be absolutely stupid to quit my high paying job?
# 
# 			I have a well paying job (low six figures) and I’ve been at my company almost a decade, but I'm burnt out from the politics.
# 			Why am I so terrified to quit? I feel like I’ve put so much time and energy into climbing high in the pay scale and wherever
# 			I end up is going to be just as bad."
# 		),
# 		assistant = list(
# 			list(
# 				employment_status = list('employed',  'The user states they have a "current job".'),
# 				recently_seperated = list('resigned',  'The user is resigning from their current job for a higher paying job.'),
# 				job_search_status = list('received offer/started new job',  'The user has received "an offer" at a new job.'),
# 				recently_received_pay_increase = list('yes - significant',  'The user accepted an offer for "3 times" their previous salary.'),
# 				includes_pay_complaint = list('yes',  'The user mentions they\'re currently "underpaid".')
# 			),
# 			list(
# 				employment_status = list('unemployed',  'The user states they are "Unemployed and Depressed".'),
# 				recently_seperated = list('fired/laid off',  'The user was laid off at McDonald\'s.'),
# 				job_search_status = list('searching/considering search',  'The user has submitted "over 1300 applications".'),
# 				recently_received_pay_increase = list('no/unknown',  'The user is unemployed and has no pay.'),
# 				includes_pay_complaint = list('no',  'The user makes no mention of pay or salary.')
# 			),
# 			list(
# 				employment_status = list('unknown',  'It is unclear whether the user is currently employed.'),
# 				recently_seperated = list('no/unknown',  'It is unclear whether the user recently left a job.'),
# 				job_search_status = list('searching/considering search',  'The user mentions applying to "100 jobs".'),
# 				recently_received_pay_increase = list('no/unknown',  'It is unclear whether the user recently received any pay increases.'),
# 				includes_pay_complaint = list('no',  'The user makes no mention of pay or salary.')
# 			),
# 			list(
# 				employment_status = list('employed', 'The user mentions currently having a "well paying job".'),
# 				recently_seperated = list('considering seperation', 'The user is considering leaving their current job.'),
# 				job_search_status = list('searching/considering search', 'The user is considering the possibility of getting a "job in a different field".'),
# 				recently_received_pay_increase = list('no/unknown', 'It is unclear whether the user recently received any pay increases.'),
# 				includes_pay_complaint = list('no', 'The user seems satisfied with their current pay.')
# 			)
# 		),
# 		user = list(
# 			"I'm burnt out but I need a new job
# 
# 			This job search has got me on edge. I started applying in October with no offers from the 12+ places
# 			that I've interviewed for. I'm tired and taking a much-needed short vacation this week although I
# 			don't actually have the time. I currently have 10 tasks (reports, etc) on
# 			my plate. I got a measly raise which is truly a slap in the face for all the work that is currently on my shoulders.",
# 			"Job asked me to come back 3 months after layoff
# 
# 			Yesterday, I got a call from the head of my old company asking if I would be interested in coming back. This was honestly the last thing I expected.
# 			I mentioned pay would be a big factor and they said they would be open to better compensation.
# 			They would also benefit from not having to train a new employee. Should I go back until I find something better?
# 			It would bring money while I have no income so Im leaning towards accepting."
# 		),
# 		assistant = list(
# 			list(
# 				employment_status = list('employed', 'The user is currently employed, as indicated by their mention of "10 tasks".'),
# 				recently_seperated = list('considering seperation', 'The user wants to leave their current job.'),
# 				job_search_status = list('searching/considering search', 'The user mentions they "started applying in October".'),
# 				recently_received_pay_increase = list('yes - small/unknown size', 'The user mentions they "got a measly raise".'),
# 				includes_pay_complaint = list('no', 'The user is dissatisfied with their pay, as they complained about "a measly raise".')
# 			),
# 			list(
# 				employment_status = list('unemployed', 'The user is unemployed and has "no income".'),
# 				recently_seperated = list('fired/laid off', 'The user was laid off 3 months ago.'),
# 				job_search_status = list('searching/considering search', 'The user seems to have been searching for jobs until they received this offer.'),
# 				recently_received_pay_increase = list('yes - small/unknown size',  'The user mentions the possibility of "better compensation", but does not specify how large it is.'),
# 				includes_pay_complaint = list('no', 'The user discusses compensation, but does not complain about it.')
# 			)
# 		)
# 		) %>%
# 		imap(., \(x, i) list(
# 			role = i,
# 			content = {
# 				if (i == 'system') str_replace_all(x, '\\t', '')
# 				else if (i == 'user') toJSON(
# 					map_chr(x, \(m)
# 						m %>%
# 							str_replace_all(., '\\t', '') %>%
# 							# Remove all linebreaks except double linebreaks
# 							str_replace_all(., '\n(?!\\n)', '_PLACEHOLDER_') %>%
# 							str_replace_all(., '\n\n', '\n') %>%
# 							str_replace_all(., '_PLACEHOLDER_', '')
# 					),
# 					auto_unbox = T
# 				)
# 				else toJSON(x, auto_unbox = T)
# 			}
# 		)) %>%
# 		unname(.)
# 
# 	# Validate prompts
# 	validate_df =
# 		base_prompts %>%
# 		keep(\(x) x$role == 'assistant') %>%
# 		map(\(x) fromJSON(x$content, simplifyVector  = F)) %>%
# 		unlist(., recursive = F) %>%
# 		imap(., \(example, example_idx)
# 			 list_rbind(imap(example, \(m, j) tibble(idx = example_idx, key = j, value = m[[1]], rationale = m[[2]])))
# 		) %>%
# 		list_rbind(.)
# 
# 	# Verify no NAs
# 	validate_df %>%
# 		count(., key, value) %>%
# 		inner_join(., label_options, by = c('key', 'value')) %>%
# 		print()
# 
# 	labor_market_base_prompts <<- base_prompts
# 	labor_market_label_options <<- label_options
# })

## LLM Calls  --------------------------------------------------------
# local({
# 
# 	base_prompt_tokens = 1600
# 	max_prompt_tokens = 3950
# 	user_prompt_tokens = max_prompt_tokens - base_prompt_tokens
# 
# 	count_res = head(reduce(labor_market_samples$n_tokens, function(accum, c) {
# 		prev = tail(accum, 1)[[1]]
# 		res = {if (prev[[2]] + c >= user_prompt_tokens) c(prev[[1]] + 1, c) else c(prev[[1]], prev[[2]] + c)}
# 		return(c(accum, list(res)))
# 		},
# 		.init = list(c(1, 0)) # First val = group index, second val = token count
# 		), -1)
# 
# 	# Batch
# 	data_to_score =
# 		labor_market_samples %>%
# 		mutate(
# 			.,
# 			score_group = map_int(count_res, \(x) x[1]),
# 			group_cum_token_count = map_dbl(count_res, \(x) x[2])
# 			)
# 
# 	user_prompts =
# 		data_to_score %>%
# 		mutate(., user_message = paste0(title, '\n', str_replace_all(selftext, "\\t|\\n", " "))) %>%
# 		group_split(., score_group) %>%
# 		imap(., \(x, i) list(
# 			score_group = unique(x$score_group),
# 			scrape_ids = x$scrape_id,
# 			post_ids = x$post_id,
# 			user_message = toJSON(x$user_message, auto_unbox = T)
# 		))
# 
# 	requests = map(user_prompts, \(p) {
# 		request('https://api.openai.com/v1/chat/completions') %>%
# 			req_headers(
# 				'Authorization' = paste0('Bearer ', Sys.getenv('OPENAI_API_KEY')),
# 				'Content-Type' = 'application/json'
# 			) %>%
# 			req_body_raw(toJSON(list(
# 				model = 'gpt-3.5-turbo',
# 				messages = c(
# 					labor_market_base_prompts,
# 					list(list(role = 'user', content = p$user_message))
# 				)
# 			), auto_unbox = T)) %>%
# 			req_timeout(60 * 10)
# 		})
# 
# 	http_responses = send_async_requests(requests, .chunk_size = 40, .max_retries = 4, .verbose = T)
# 
# 	res = list_rbind(imap(http_responses, function(r, i) {
# 
# 		response_content = resp_body_json(r)$choices[[1]]$message$content
# 
# 		# Throw non-JSON outputs
# 		if (validate(response_content) == F) return(NULL)
# 
# 		post_ids = user_prompts[[i]]$post_ids
# 		scrape_ids = user_prompts[[i]]$scrape_ids
# 		json_parsed = fromJSON(response_content, simplifyVector = F)
# 
# 		# Throw out incorrect counts, since uncertain which one corresponds to what
# 		if (length(json_parsed) != length(post_ids)) return(NULL)
# 
# 		# Iterate through each and check keys, pivot longer
# 		group_gpt_results = list_rbind(compact(imap(json_parsed, function(poster_res, j) {
# 
# 			if (length(unique(labor_market_label_options$key)) != length(names(poster_res))) return(NULL)
# 
# 			if (!all(sort(unique(labor_market_label_options$key)) == sort(unique(names(poster_res))))) return(NULL)
# 
# 			if (!all(map(poster_res, \(k) length(k)) == 2)) return(NULL)
# 
# 			list_rbind(imap(poster_res, \(k, name) tibble(key = name, value = k[[1]], rationale = k[[2]]))) %>%
# 				bind_cols(scrape_id = scrape_ids[[j]], post_id = post_ids[[j]], .)
# 
# 			})))
# 
# 		return(group_gpt_results)
# 	}))
# 
# 	# Remove any post_ids where *any* of the label keys or values are invalid
# 	filtered_res =
# 		res %>%
# 		left_join(
# 			.,
# 			transmute(labor_market_label_options, key, value, label_exists = 1),
# 			by = c('key', 'value')
# 			) %>%
# 		mutate(., is_na = ifelse(is.na(label_exists), 1, 0)) %>%
# 		mutate(., post_invalid_keys_or_values = sum(is_na), .by = post_id) %>%
# 		filter(., post_invalid_keys_or_values == 0) %>%
# 		select(., -is_na, -post_invalid_keys_or_values) %>%
# 		transmute(
# 			.,
# 			scrape_id,
# 			post_id,
# 			prompt_id = labor_market_prompt_id,
# 			label_key = key,
# 			label_value = value,
# 			label_rationale = rationale
# 		)
# 
# 
# 	message('***** Unique post IDs returned: ', length(unique(filtered_res$post_id)))
# 	message('***** Unique post IDs desired: ', sum(map_int(user_prompts, \(x) length(x$post_ids))))
# 
# 	llm_outputs$labor_market <<- filtered_res
# })

"Is the user feeling financially stressed or worried? If yes, respond with a list of reasons why"
"
YES:
	- Inflation: the user is financially stressed due to high cost of living or rising prices. 
	- Unemployment: the user is financially stressed due to not having a job
	- Layoff: the user is financially stressed due to have been laid off
	- Credit: the user is financially stressed due to high debt servicing costs, such as credit card debt, student loans, etc.
	- Housing: The user is financially stressed due to housing costs (), or due to worry about not being able to afford a house.
	- Financial markets: the user is financially stressed due to performance of financial assets, such as stocks, cryptocurrency, etc.
	- FOMO: the user feels like they\'re being left behind by others or peers, in terms of income, life experiences, etc.
	- Politics: the user feels financially stressed due to political reasons.
NO:
  - 

NEUTRAL: [reason]
"


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
