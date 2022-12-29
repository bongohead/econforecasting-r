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


#' Log an entry in the run_logs Postgres table
#'
#' @description
#' Log an entry with columns corresponding to the passed parameters in the run_logs Postgres table
#'
#' @param db The database connection object.
#' @param run_id The name of the run to be logged.
#' @param job The name of the job to be logged.
#' @param module The module the job belongs to.
#' @param log_type The type of the log message. Valid options are 'job-success', 'job-start', 'job-failed', 'data-dump'.
#' @param log_dump Details of the log message - a list to be casted into a JSON string.
#'
#' @importFrom DBI dbExecute
#' @importFrom tibble tribble
#' @importFrom jsonlite toJSON
#'
#' @export
log_job_in_db = function(db, run_id, job, module, log_type, log_dump = list()) {

	if (class(db) != 'PqConnection') stop('Parameter db must be a PqConnection object!')
	if (!is.character(run_id)) stop('Parameter run_id must be a character!')
	if (!is.character(job)) stop('Parameter job must be a character!')
	if (!is.character(module)) stop('Parameter module must be a character!')
	if (!log_type %in% c('job-success', 'job-start', 'job-failed', 'data-dump')) {
		stop('Parameter log_type must be one of job-success, job-start, job-failed, data-dump. Check DDL!')
	}

	if (!is.character(log_type)) stop('Parameter log_type must be a character!')
	if (!is.list(log_dump)) stop('Parameter log_dump must be a list!')

	log_dump = {if (is.list(log_dump)) toJSON(log_dump, auto_unbox = TRUE) else log_dump}

	log_entry = tribble(
		~ run_id, ~ job, ~ module, ~ log_type, ~ log_dump, ~ is_interactive,
		run_id, job, module, log_type, log_dump, interactive()
		)

	dbExecute(db, create_insert_query(
		log_entry,
		'run_logs',
		'ON CONFLICT ON CONSTRAINT run_logs_pk DO UPDATE
		SET
			job=EXCLUDED.job,
			module=EXCLUDED.module,
			log_type=EXCLUDED.log_type,
			log_dump=EXCLUDED.log_dump,
			is_interactive=EXCLUDED.is_interactive,
			created_at=CURRENT_TIMESTAMP'
		))

	return(TRUE)
}


#' Log a start in the run_logs table and return the run_id
#'
#' @param db The database connection object.
#' @param run_id The name of the run to be logged.
#' @param job The name of the job to be logged.
#' @param module The module the job belongs to.
#'
#' @export
log_start_in_db = function(db, job, module) {

	run_id = as.character(round(as.numeric(as.POSIXct(Sys.time())) * 1000000))

	log_job_in_db(
		db = db,
		run_id = run_id,
		job = job,
		module = module,
		log_type = 'job-start'
	)

	return(run_id)
}

#' Log a data-dump in the run_logs table
#'
#' @param db The database connection object.
#' @param run_id The name of the run to be logged.
#' @param job The name of the job to be logged.
#' @param module The module the job belongs to.
#' @param log_dump Details of the log message - either as a JSON string or as a list to be casted into a JSON string.
#'
#' @export
log_dump_in_db = function(db, run_id, job, module, log_dump = list()) {

	log_job_in_db(
		db = db,
		run_id = run_id,
		job = job,
		module = module,
		log_type = 'data-dump',
		log_dump = log_dump
	)

	return(T)
}

#' Log a finish in the run_logs table
#'
#' @param db The database connection object.
#' @param run_id The name of the run to be logged.
#' @param job The name of the job to be logged.
#' @param module The module the job belongs to.
#' @param log_dump Details of the log message - either as a JSON string or as a list to be casted into a JSON string.
#'
#' @export
log_finish_in_db = function(db, run_id, job, module, log_dump = list()) {

	log_job_in_db(
		db = db,
		run_id = run_id,
		job = job,
		module = module,
		log_type = 'job-success',
		log_dump = log_dump
		)

	return(T)
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
#' @export
get_rowcount = function(db, tablename) {

	if (class(db) != 'PqConnection') stop('Parameter db must be a PqConnection object!')
	if (!is.character(tablename)) stop('Parameter tablename must be a character!')

	count = as.numeric(dbGetQuery(db, paste0('SELECT COUNT(*) AS count FROM ', tablename, ''))$count)

	return(count)
}
