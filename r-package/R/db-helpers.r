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

#' Disconenct frmo DB
#'
#' @param db The DBI connector object
#'
#' @importFrom DBI dbDisconnect
#'
#' @export
disconnect_db = function(db) {
	dbDisconnect(db)
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

#' Helper function to check row count of a table in Postgres
#'
#' @description
#' Returns the number of rows in a table
#'
#' @param db The database connection object.
#' @param tablename The name of the table.
#'
#' @importFrom DBI dbGetQuery
#'
#' @export
get_rowcount = function(db, tablename) {

	if (class(db) != 'PqConnection') stop('Parameter db must be a PqConnection object!')
	if (!is.character(tablename)) stop('Parameter tablename must be a character!')

	count = as.numeric(dbGetQuery(db, paste0('SELECT COUNT(*) AS count FROM ', tablename, ''))$count)

	return(count)
}

#' Create a SQL INSERT query string.
#'
#' @param df A data frame to insert into SQL.
#' @param tblname The name of the SQL table.
#' @param .append Any additional characters to append to the end of the query.
#'
#' @return A character string representing a SQL query.
#'
#' @import dplyr
#' @importFrom tidyr unite
#' @importFrom DBI dbQuoteString ANSI
#'
#' @export
create_insert_query = function(df, tblname, .append = '') {

	if (!requireNamespace('DBI', quietly = T)) stop('Package "DBI" must be installed to use this function.', call. = F)

	paste0(
		'INSERT INTO ', tblname, ' (', paste0(colnames(df), collapse = ','), ')\n',
		'VALUES\n',
		df %>%
			mutate_if(is.Date, as.character) %>%
			# dplyr::mutate_if(is.character, function(x) paste0("'", x, "'")) %>%
			mutate_if(is.character, function(x) dbQuoteString(ANSI(), x)) %>%
			mutate_if(is.numeric, function(x) dbQuoteString(ANSI(), as.character(x))) %>%
			unite(., 'x', sep = ',') %>%
			mutate(., x = paste0('(', x, ')')) %>%
			.$x %>%
			paste0(., collapse = ', '), '\n',
		.append, ';'
		) %>%
		return(.)
}


#' Store forecast values in forecast values table
#'
#' This breaks up the dataframe into chunks of size 10000 and sends them into the database.
#'
#' @param db The database conenction object.
#' @param df The dataframe of forecast values. Must include columns forecast, vdate, form, freq, varname, and date.
#' @param .verbose Echo error messages.
#'
#' @return The number of rows added
#'
#' @importFrom dplyr mutate group_split
#' @importFrom purrr map_dbl
#' @importFrom DBI dbExecute
#'
#' @export
store_forecast_values_v1 = function(db, df, .verbose = F) {

	if (class(db) != 'PqConnection') stop('Parameter "db" must be a PqConnection object!')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (
		length(colnames(df)) != 7 ||
		!all(sort(colnames(df)) == sort(c('forecast', 'vdate', 'freq', 'form', 'varname', 'date', 'value')))
		) {
		stop('Incorrect columns')
	}

	initial_count = get_rowcount(db, 'forecast_values')
	if (.verbose == T) message('***** Initial Count: ', initial_count)

	insert_groups = group_split(mutate(df, split = ceiling((1:nrow(df))/10000)), split, .keep = F)

	insert_result = map_dbl(insert_groups, .progress = .verbose, function(x)
		dbExecute(db, create_insert_query(
			x,
			'forecast_values',
			'ON CONFLICT (forecast, vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
			))
		)

	insert_result = {if (any(is.null(insert_result))) stop('SQL Error!') else sum(insert_result)}

	final_count = get_rowcount(db, 'forecast_values')
	rows_added = final_count - initial_count
	if (.verbose == T) message('***** Rows Added: ', rows_added)

	return(rows_added)
}


#' Store forecast values in forecast_values_v2 table
#'
#' This breaks up the dataframe into chunks of size 10000 and sends them into the database.
#'
#' @param db The database conenction object.
#' @param df The dataframe of forecast values. Must include columns forecast, vdate, form, freq, varname, and date.
#' @param .store_new_only Boolean; whether to *only* store rows where the combination of (forecast, vdate) doesn't already exist in the database. Defaults to F.
#' @param .verbose Echo error messages.
#'
#' @return The number of rows added
#'
#' @importFrom dplyr mutate group_split
#' @importFrom purrr map_dbl
#' @importFrom DBI dbExecute
#' @importFrom lubridate today
#'
#' @export
store_forecast_values_v2 = function(db, df, .store_new_only = F, .verbose = F) {

	if (class(db) != 'PqConnection') stop('Parameter "db" must be a PqConnection object!')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (
		length(colnames(df)) != 7 ||
		!all(sort(colnames(df)) == sort(c('forecast', 'vdate', 'freq', 'form', 'varname', 'date', 'value')))
		) {
		stop('Incorrect columns')
	}

	initial_count = get_rowcount(db, 'forecast_values_v2')
	if (.verbose == T) message('***** Initial Count: ', initial_count)

	today_string = today('US/Eastern')

	existing_forecast_combinations = get_query(db, paste0("SELECT forecast, vdate FROM forecast_values_v2 GROUP BY forecast, vdate"))
	store_df = anti_join(df, existing_forecast_combinations, by = c('forecast', 'vdate'))

	if (nrow(store_df) > 0) {

		insert_groups = group_split(mutate(store_df, mdate = today_string, split = ceiling((1:nrow(store_df))/10000)), split, .keep = F)

		insert_result = map_dbl(insert_groups, .progress = .verbose, function(x)
			dbExecute(db, create_insert_query(
				x,
				'forecast_values_v2',
				'ON CONFLICT (mdate, forecast, vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
			))
		)

		insert_result = {if (any(is.null(insert_result))) stop('SQL Error!') else sum(insert_result)}
	}


	final_count = get_rowcount(db, 'forecast_values_v2')
	rows_added = final_count - initial_count


	if (.verbose == T) message('***** Rows Added: ', rows_added)

	if (rows_added != 0) {

		if (.verbose == T) message('***** Refreshing Views')
		initial_count_all = get_rowcount(db, 'forecast_values_v2_all')
		initial_count_latest = get_rowcount(db, 'forecast_values_v2_latest')
		dbExecute(db, 'REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_all;')
		dbExecute(db, 'REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_values_v2_latest;')
		rows_added_all = get_rowcount(db, 'forecast_values_v2_all') - initial_count_all
		rows_added_latest = get_rowcount(db, 'forecast_values_v2_latest') - initial_count_latest

	} else {
		rows_added_all = 0
		rows_added_latest = 0

	}

	return(list(forecast_values = rows_added, forecast_values_all = rows_added_all, forecast_values_latest = rows_added_latest))
}



#' Store forecast values in forecast_hist_values_v2 table
#'
#' This breaks up the dataframe into chunks of size 10000 and sends them into the database.
#'
#' @param db The database conenction object.
#' @param df The dataframe of forecast values. Must include columns forecast, vdate, form, freq, varname, and date.
#' @param .verbose Echo error messages.
#'
#' @return The number of rows added
#'
#' @importFrom dplyr mutate group_split
#' @importFrom purrr map_dbl
#' @importFrom DBI dbExecute
#' @importFrom lubridate today
#'
#' @export
store_forecast_hist_values_v2 = function(db, df, .verbose = F) {

	if (class(db) != 'PqConnection') stop('Parameter "db" must be a PqConnection object!')
	if (!is.data.frame(df)) stop('Parameter "df" must be a data.frame.')
	if (
		length(colnames(df)) != 6 ||
		!all(sort(colnames(df)) == sort(c('vdate', 'freq', 'form', 'varname', 'date', 'value')))
	) {
		stop('Incorrect columns')
	}

	initial_count = get_rowcount(db, 'forecast_hist_values_v2')
	if (.verbose == T) message('***** Initial Count: ', initial_count)

	today_string = today('US/Eastern')

	insert_groups = group_split(mutate(df, split = ceiling((1:nrow(df))/10000)), split, .keep = F)

	insert_result = map_dbl(insert_groups, .progress = .verbose, function(x)
		dbExecute(db, create_insert_query(
			x,
			'forecast_hist_values_v2',
			'ON CONFLICT (vdate, form, freq, varname, date) DO UPDATE SET value=EXCLUDED.value'
		))
	)

	insert_result = {if (any(is.null(insert_result))) stop('SQL Error!') else sum(insert_result)}

	final_count = get_rowcount(db, 'forecast_hist_values_v2')
	rows_added = final_count - initial_count
	if (.verbose == T) message('***** Rows Added: ', rows_added)

	if (rows_added != 0) {
		if (.verbose == T) message('***** Refreshing Views')
		initial_count_latest = get_rowcount(db, 'forecast_hist_values_v2_latest')
		dbExecute(db, 'REFRESH MATERIALIZED VIEW CONCURRENTLY forecast_hist_values_v2_latest;')
		rows_added_latest = get_rowcount(db, 'forecast_hist_values_v2_latest') - initial_count_latest
	} else {
		rows_added_latest = 0
	}

	return(list(forecast_hist_values = rows_added, forecast_hist_values_latest = rows_added_latest))
}
