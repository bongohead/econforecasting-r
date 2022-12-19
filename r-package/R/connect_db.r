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


#' Log an entry in the job_logs Postgres table
#'
#' @description
#' Log an enry with columns corresponding to the passed parameters in the job_logs Postgres table
#'
#' @param db The database connection object
#' @param job The name of the job to be logged
#' @param module The module the job belongs to
#' @param type The type of the log message
#' @param details Details of the log message as a JSON string
#'
#' @importFrom DBI dbExecute
#' @importFrom tibble tribble
#' @export
log_job_in_db = function(db, job, module, type = 'job-success', details) {

	if (class(db) == 'PqConnection') stop('Parameter db must be a PqConnection object!')
	if (!is.character(job)) stop('Parameter job must be a character!')
	if (!is.character(module)) stop('Parameter module must be a character!')
	if (!is.character(type)) stop('Parameter class must be a character!')
	if (!is.character(details)) stop('Parameter details must be a character vector!')

	log_entry = tribble(
		~ logname, ~ module, ~ log_date, ~ log_group, ~ log_info,
		job, module, today(), type, details
		)

	dbExecute(create_insert_query(
		log_entry,
		'job_logs',
		'ON CONFLICT ON CONSTRAINT job_logs_pk DO UPDATE SET log_info=EXCLUDED.log_info,log_dttm=CURRENT_TIMESTAMP'
		))

	return(TRUE)
}

