#' Get a list of standard headers
#'
#' @description
#' Gets a vector of standard headers to pass into add_headers() in an httr request
#'
#' @param args_list Headers to append to the default;
#'  a named vector of key value pairs corresponding to headers, e.g. c('Cache-Control' = 'no-cache')
#'
#' @export
get_standard_headers = function(args_list = c()) {

	random_user_agent = sample(c(
		# Chrome W10
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
		# Firefox Linux
		'Mozilla/5.0 (X11; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0',
		# Firefox Windows
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0'
	), 1)

	default_headers = c(
		'User-Agent' = random_user_agent,
		'Accept-Encoding' = 'gzip, deflate, br',
		'Accept-Language' ='en-US,en;q=0.5',
		'Cache-Control'='no-cache',
		'Connection'='keep-alive',
		'Pragma' = 'no-cache',
		'DNT' = '1'
	)

	joined_headers = c(args_list, default_headers[!names(default_headers) %in% names(args_list)])

	joined_headers
}

#' Get a list of standard headers
#'
#' @description
#' Adds a vector of standard headers into httr2 request object.
#'  Call req_headers() afterwards to add additional headers; note that headers here will be overwritten if the
#'  names are identical.
#'
#' @param req An httr2 request object
#'
#' @importFrom httr2 req_headers
#'
#' @export
add_standard_headers = function(req) {

	if (class(req) != 'httr2_request') stop('Argument "req" must be an httr2 request object!')

	random_user_agent = sample(c(
		# Chrome W10
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
		# Firefox Linux
		'Mozilla/5.0 (X11; Linux x86_64; rv:107.0) Gecko/20100101 Firefox/107.0',
		# Firefox Windows
		'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:107.0) Gecko/20100101 Firefox/107.0'
	), 1)

	req2 = req_headers(
		req,
		'User-Agent' = random_user_agent,
		'Accept-Encoding' = 'gzip, deflate, br',
		'Accept-Language' ='en-US,en;q=0.5',
		'Cache-Control'='no-cache',
		'Connection'='keep-alive',
		'Pragma' = 'no-cache',
		'DNT' = '1'
	)

	return(req2)
}

#' Get cookies from an httr2 response object
#'
#' @param split Boolean; set T to split each cookie, F to return a single character
#'
#' @export
get_cookies = function(response, split = T) {

	if (class(response) != 'httr2_response') stop('Argument "response" must be an httr2 response object!')

	cookie_objs = (response$headers[names(response$headers) == 'set-cookie'])
	cookie_names = str_extract(as.character(cookie_objs), "[^=]+")
	cookie_bodies = str_extract(as.character(cookie_objs), "(?<==).*")
	cookie_values = sapply(str_split(cookie_bodies, ';'), \(x) x[[1]])

	if (split == T) {

		return(setNames(as.list(paste0(cookie_names, '=', cookie_values)), cookie_names))

	} else {

		return(paste0(paste0(cookie_names, '=', cookie_values), collapse = '; '))
	}

}


#' Helper to resend a batch of requests, used for async request functions
#'
#' @param requests_to_send A list of `httr2` request objects to send.
#' @param .retries The number of retry attempts before failing.
#' @param .pool A pool object returns by `curl:::new_pool`.
#' @param .verbose If TRUE, outputs error statuses.
#'
#' @import dplyr purrr httr2
#' @importFrom curl new_pool
#'
#' @noRd
retry_requests = function(requests_to_send, .retries = 0L, .max_retries = 5L, .pool = new_pool(total_con = 4, host_con = 4, multiplex = T), .verbose = T) {

	stopifnot(
		is_list(requests_to_send),
		is_scalar_integer(.retries) | is_scalar_double(.retries),
		is_scalar_logical(.verbose)
	)

	if (.retries > .max_retries) stop('Requests failed')
	if (.retries > 0) {
		message('Retry ', .retries, ' for ', length(requests_to_send), ' failed requests ')
		Sys.sleep(2 * 1.8 ^ .retries)
	}

	responses = setNames(
		multi_req_perform(reqs = requests_to_send, pool = .pool, cancel_on_error = F),
		names(requests_to_send)
	)

	success_response_ids = names(keep(responses, \(x) 'httr2_response' %in% class(x)))
	failure_response_ids = names(keep(responses, \(x) !'httr2_response' %in% class(x)))

	if (length(requests_to_send[failure_response_ids]) > 0) {

		if (.verbose) {
			print('Failed requests!')
			print(requests_to_send[failure_response_ids])
		}
		retry_responses = retry_requests(requests_to_send[failure_response_ids], .retries = .retries + 1, .pool = .pool)

	} else {

		retry_responses = list()
	}

	all_responses = c(responses[success_response_ids], retry_responses)

	return(all_responses)
}

#' Send multiple HTTP requests asynchronously
#'
#' @param reqs_list A list of `httr2` request objects to send.
#' @param .chunk_size The number of requests to send in a single asynchronous batch.
#' @param .max_conn The maximum number of connections to establish; equal to .chunk_size by default.
#' @param .max_retries The maximum number of retries to attempt before erroring out.
#' @param .verbose If TRUE, outputs error statuses and progress.
#'
#' @return An `httr2` response object.
#'
#' @description
#' Sends asynchronous requests with error handling for retreies. Up to `chunk_size` requests are processed asynchronously.
#' The returned object preserves the original order of the requests.
#'
#' @examples
#' send_async_requests(list(
#'	request('https://www.reuters.com/news/archive?view=page&page=1&pageSize=10'),
#'  request('https://www.reuters.com/news/archive?view=page&page=2&pageSize=10')
#' ))
#'
#' @import dplyr purrr httr2
#' @importFrom curl new_pool
#'
#' @export
send_async_requests = function(reqs_list, .chunk_size = 10, .max_conn = .chunk_size, .max_retries = 5, .verbose = T) {

	stopifnot(
		is_list(reqs_list) | length(reqs_list) == 0 | 'httr2_request' %in% class(reqs_list[[1]]),
		is_scalar_integer(.chunk_size) | is_scalar_double(.chunk_size),
		is_scalar_integer(.max_conn) | is_scalar_double(.max_conn),
		is_scalar_integer(.max_retries) | is_scalar_double(.max_retries),
		is_scalar_logical(.verbose)
	)

	reqs_list_named = setNames(reqs_list, paste0('c', 1:length(reqs_list)))
	reqs_chunked = unname(split(reqs_list_named, (1:length(reqs_list_named) - 1) %/% .chunk_size))

	parsed_responses = map(reqs_chunked, .progress = .verbose, function(requests_chunk) {
		return(retry_requests(
			requests_chunk,
			.verbose = .verbose,
			.retries = 0,
			.max_retries = .max_retries,
			.pool = new_pool(total_con = .max_conn, host_con = .max_conn, multiplex = T)
			))
	})

	# Collapse and order by c1, c2, ...
	res_collapsed = unname(unlist(unname(parsed_responses), recursive = F)[names(reqs_list_named)])

	if(length(res_collapsed) != length(reqs_list)) stop('Error: length output not same as input!')

	return(res_collapsed)
}
