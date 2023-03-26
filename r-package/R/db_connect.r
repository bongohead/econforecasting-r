#' Connect to main Postgres database
#'
#' @importFrom RPostgres Postgres
#' @importFrom DBI dbConnect
#'
#' @export
connect_pg = function() {

	check_env_variables(c('DB_DATABASE', 'DB_SERVER', 'DB_USERNAME', 'DB_PASSWORD', 'DB_PORT'))

	db = dbConnect(
		RPostgres::Postgres(),
		dbname = Sys.getenv('DB_DATABASE'),
		host = Sys.getenv('DB_SERVER'),
		port = Sys.getenv('DB_PORT'),
		user = Sys.getenv('DB_USERNAME'),
		password = Sys.getenv('DB_PASSWORD')
	)

	return(db)
}

#' Execute a select query
#'
#' @param db The DBI connector object
#' @param query The query
#'
#' @importFrom DBI dbGetQuery
#'
#' @export
get_query = function(db, query) {

	if (!'PqConnection' %in% class(db)) stop('Object "db" must be of class PgConnection')

	return(as_tibble(dbGetQuery(db, query)))
}

#' Check largest table sizes in Postgres database.
#'
#' @param db A PostgreSQL DBI connection.
#'
#' @return A data frame of table sizes.
#'
#' @import dplyr
#'
#' @export
get_pg_table_sizes = function(db) {

	if (!'PqConnection' %in% class(db)) stop('Object "db" must be of class PgConnection')

	res = get_query(db, sql(
		"SELECT
		schema_name,
		relname,
		pg_size_pretty(table_size) AS size,
		table_size

		FROM (
			SELECT
				pg_catalog.pg_namespace.nspname AS schema_name,
				relname,
				pg_relation_size(pg_catalog.pg_class.oid) AS table_size
			FROM pg_catalog.pg_class
			JOIN pg_catalog.pg_namespace ON relnamespace = pg_catalog.pg_namespace.oid
		) t
		WHERE schema_name NOT LIKE 'pg_%'
		ORDER BY table_size DESC;"
	))
}
