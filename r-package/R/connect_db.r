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


#' Log an entry in the model_logs Postgres table
#'
#' @description
#' Log an enry with columns corresponding to the passed parameters in the model_logs Postgres table
#'
#' @param db The database connection object.
#' @param jobname The name of the job to be logged.
#' @param module The module the job belongs to.
#' @param log_type The type of the log message. Valid options are 'job-success', 'job-start', 'job-failed', 'data-dump'.
#' @param log_dump Details of the log message as a JSON string.
#' @param log_subtype The subtype. Optional, usually only used for log_type = 'data-dump'
#'
#' @importFrom DBI dbExecute
#' @importFrom tibble tribble
#' @export
log_job_in_db = function(db, jobname, module, log_type, log_dump = NA_character_, log_subtype = NA_character_) {

	if (class(db) != 'PqConnection') stop('Parameter db must be a PqConnection object!')
	if (!is.character(jobname)) stop('Parameter jobname must be a character!')
	if (!is.character(module)) stop('Parameter module must be a character!')
	if (!log_type %in% c('job-success', 'job-start', 'job-failed', 'data-dump')) {
		stop('Parameter log_type must be one of job-success, job-start, job-failed, data-dump. Check DDL!')
	}

	if (!is.character(log_type)) stop('Parameter log_type must be a character!')
	if (!is.na(log_subtype) && !is.character(log_subtype)) stop('Parameter log_subtype must be a character!')
	if (!is.character(log_dump)) stop('Parameter log_dump must be a character vector!')

	log_entry = tribble(
		~ jobname, ~ module, ~ log_type, ~ log_subtype, ~ log_dump,
		jobname, module, log_type, log_subtype, log_dump
		)

	dbExecute(db, create_insert_query(
		log_entry,
		'model_logs',
		'ON CONFLICT ON CONSTRAINT model_logs_pk DO UPDATE
		SET log_dump=EXCLUDED.log_dump,log_subtype=EXCLUDED.log_subtype,created_at=CURRENT_TIMESTAMP'
		))

	return(TRUE)
}


#' Helper function to check row count of a table in Postgres
#'
#' @description
#' Returns the number of rows in a table
#'
#' @param db The database connection object.
#' @param tablename The name of the table.
#'
#' @importFrom DBI dbGetQuery
get_rowcount = function(db, tablename) {

	if (class(db) != 'PqConnection') stop('Parameter db must be a PqConnection object!')
	if (!is.character(tablename)) stop('Parameter tablename must be a character!')

	count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM interest_rate_model_input_values')$count)

	return(count)
}
