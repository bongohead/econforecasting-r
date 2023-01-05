#' Connect to main Postgres/TimescaleDB database
#'
#' @description
#' Connect to Postgres/TimescaleDB database. You can either pass secrets directly through a list,
#'  a YAML filepath, or seperate named arguments.
#'
#' @param secrets A list with database secrets key-value pairs
#' @param secrets_path A filepath to a constants YAML file with database secrets key-value pairs
#' @param ... Optional arguments with database secret key-value pairs
#'
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect
#' @export
connect_db = function(secrets_list = NULL, secrets_path = NULL, ...) {

	args = list(...)

	if (any(c('DB_DATABASE', 'DB_SERVER', 'DB_USERNAME', 'DB_PASSWORD', 'DB_PORT') %in% names(args))) {
		if (!all(c('DB_DATABASE', 'DB_SERVER', 'DB_USERNAME', 'DB_PASSWORD', 'DB_PORT') %in% names(args))) {
			stop('Pass secrets, secrets_path, or all of DB_DATABASE, DB_SERVER, DB_USERNAME, DB_PASSWORD, and DB_PORT.')
		}
		secrets = list(
			DB_DATABASE = args$DB_DATABASE,
			DB_SERVER = args$DB_SERVER,
			DB_PORT = args$DB_PORT,
			DB_USERNAME = args$DB_USERNAME,
			DB_PASSWORD = args$DB_PASSWORD
		)
	} else if (!is.null(secrets_list)) {
		if (!all(c('DB_DATABASE', 'DB_SERVER', 'DB_USERNAME', 'DB_PASSWORD', 'DB_PORT') %in% names(secrets_list))) {
			stop('Secrets list must include DB_DATABASE, DB_SERVER, DB_USERNAME, DB_PASSWORD, and DB_PORT.')
		}
		secrets = secrets_list
	} else if (!is.null(secrets_path)) {
		secrets = get_secrets(secrets_path, c('DB_DATABASE', 'DB_SERVER', 'DB_USERNAME', 'DB_PASSWORD', 'DB_PORT'))
	} else {
		stop('Pass secrets, secrets_path, or all of DB_DATABASE, DB_SERVER, DB_USERNAME, DB_PASSWORD, and DB_PORT.')
	}


	db = dbConnect(
		Postgres(),
		dbname = secrets$DB_DATABASE,
		host = secrets$DB_SERVER,
		port = secrets$DB_PORT,
		user = secrets$DB_USERNAME,
		password = secrets$DB_PASSWORD
	)

	return(db)
}
