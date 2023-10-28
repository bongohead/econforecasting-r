#' Returns the top Reddit posts for a given subreddit
#'
#' @param subreddit The name of the subreddit, e.g. `dogs`.
#' @param freq The frequency of the aggregation - valid values are `day`, `week`, `month`, `year`, and `all`.
#' @param ua The `User-Agent` to pass as a header, should be of the form `<platform>:<app ID>:<version string>
#'  (by u/<Reddit username>`. See [https://praw.readthedocs.io/en/stable/getting_started/quick_start.html].
#' @param token The auth token fetched from [https://www.reddit.com/api/v1/access_token].
#' @param max_pages The maximum amount of pages to iterate through. There are 100 results per page.
#'  Defaults to 10.
#' @param min_upvotes The minimum number of upvotes to retain a post. Defaults to 0
#' @param .verbose Logging verbosity; defaults to FALSE.
#'
#' @return A data frame of data with each row representing a post
#'
#' @description
#' Uses the Reddit API to iterate through results to pull top posts for a given `subreddit` and `freq`
#'  (e.g., the top posts by week for the "dogs" subreddit).
#'
#' @examples
#' get_reddit_data_by_board('StockMarket', freq = 'day', ua = reddit_ua, token = reddit_token, max_pages = 1)
#'
#'
#' @import dplyr purrr
#' @importFrom httr2 request req_headers req_perform resp_headers resp_body_json
#'
#' @export
get_reddit_data_by_board = function(subreddit, freq, ua, token, max_pages = 10, min_upvotes = 0, .verbose = F) {

	stopifnot(
		is_scalar_character(subreddit),
		is_scalar_character(freq) & freq %in% c('day', 'week', 'month', 'year', 'all'),
		is_scalar_character(ua),
		is_scalar_character(token),
		is_scalar_double(min_upvotes) | is_scalar_integer(min_upvotes),
		is_scalar_double(min_upvotes) | is_scalar_integer(min_upvotes),
		is_scalar_logical(.verbose)
	)

	# Only top possible for top
	reduce(1:max_pages, function(accum, i) {

		if (.verbose) message('***** Pull ', i)

		query =
			list(t = freq, limit = 100, show = 'all', after = {if (i == 1) NULL else tail(accum, 1)$after}) %>%
			compact(.) %>%
			paste0(names(.), '=', .) %>%
			paste0(collapse = '&')

		# message(query)
		http_result =
			request(paste0('https://oauth.reddit.com/r/', subreddit, '/top?', query)) %>%
			req_headers('User-Agent' = ua, 'Authorization' = paste0('bearer ', token)) %>%
			req_perform(.)

		if (as.integer(resp_headers(http_result)$`x-ratelimit-remaining`) == 0) {
			Sys.sleep(as.integer(resp_headers(http_result)$`x-ratelimit-reset`))
		}

		result = resp_body_json(http_result)

		parsed =
			map(result$data$children, \(post) as_tibble(compact(keep(post$data, \(z) !is.list(z))))) %>%
			list_rbind() %>%
			select(., any_of(c(
				'name', 'subreddit', 'title', 'created',
				'selftext', 'upvote_ratio', 'ups', 'is_self', 'domain', 'url_overridden_by_dest'
			))) %>%
			mutate(., i = i, after = coalesce(result$data$after, NA_character_))

		if (is.null(result$data$after)) {
			if (.verbose) message('----- Break, missing AFTER')
			return(done(bind_rows(list(accum, parsed))))
		} else {
			return(bind_rows(list(accum, parsed)))
		}

	}, .init = tibble()) %>%
	filter(., ups >= min_upvotes)

}

