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
