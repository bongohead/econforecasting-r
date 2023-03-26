#' Check largest table sizes in Postgres database.
#'
#' @param conn A PostgreSQL DBI connection.
#'
#' @return A data frame of table sizes.
#'
#' @import dplyr

#' @export
check_table_sizes = function(conn) {

	if (!requireNamespace('DBI', quietly = T)) stop('Package "DBI" must be installed to use this function.', call. = F)

	DBI::dbGetQuery(
		conn,
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
	)
}
