#' Load env file
#'
#' @param env_path The path to the .env file.
#'
#' @importFrom dotenv load_dot_env
#'
#' @export
load_env = function(project_path) {

	load_dot_env(file.path(project_path, '.env'))

	return(T)
}

#' Check if the passed variables exist in the system environment
#'
#' @param variables A character vector of variables to verify exist in the system env.
#'
#' @export
check_env_variables = function(variables) {

	if (!is.character(variables)) stop('Argument "variables" must be a character or character vector')

	missing_variables = variables[!variables %in% names(Sys.getenv())]

	if (length(missing_variables) != 0) {
		stop(paste0(
			'Missing variables in system env: ',
			paste0(missing_variables, collapse = ', '),
			'. ',
			'Try setting variables in a .env file in the project root and call it with load_env().'
		))
	}

	return(T)
}

#' Log the output to a file
#'
#' @param log_path A character representing the filename of the log file.
#'
#' @export
send_output_to_log = function(log_path) {

	check_env_variables('EF_DIR')

	sink_path = file.path(log_path)
	sink_conn = file(sink_path, open = 'at')

	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))

	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

	return(T)
}
