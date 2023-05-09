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
