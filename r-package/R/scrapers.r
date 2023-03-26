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

#' Sends parallel requests with retry
#'
#' @param reqs A list of httr2 request objects
#' @param .allowed_retries The max allowance of retries
#' @param .pool A curl pool object created by curl::new_pool()
#'
#' @import httr2
#' @importFrom curl new_pool
#' @importFrom purrr keep
#'
#' @export
send_parallel_requests = function(reqs, .allowed_retries = 10, .pool = curl::new_pool(total_con = 4, host_con = 4, multiplex = T)) {

	reqs = setNames(reqs, paste0('r', 1:length(reqs)))

	retry_requests = function(requests_to_send, .total_retries = 0) {

		if (.total_retries > .allowed_retries) stop('Requests failed')
		if (.total_retries > 0) {
			message('Retry ', .total_retries, ' for ', length(requests_to_send), ' failed_requests')
			Sys.sleep(2 * 2 ^ .total_retries)
		}

		responses = setNames(
			multi_req_perform(requests_to_send, pool = .pool, cancel_on_error = F),
			names(requests_to_send)
		)

		success_response_ids = names(keep(responses, \(x) 'httr2_response' %in% class(x)))
		failure_response_ids = names(keep(responses, \(x) !'httr2_response' %in% class(x)))

		if (length(requests_to_send[failure_response_ids]) > 0) {

			retry_responses = retry_requests(requests_to_send[failure_response_ids], .total_retries = .total_retries + 1)

		} else {

			retry_responses = list()
		}

		all_responses = c(responses[success_response_ids], retry_responses)

		return(all_responses)
	}

	return(retry_requests(reqs))
}
