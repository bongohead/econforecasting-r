#' Create fully external forecasts -> partially external forecasts

# Set Table Contents ---------------------------------------------------------

## Variables Table ---------------------------------------------------------
external_import_variables = tribble(
	~ varname, ~ fullname, ~ units,
	'gdp', 'Gross Domestic Product', 'Annualized % Change',
	
)

# Variables Table ---------------------------------------------------------

	

	
	)

external_import_sources = tribble(
	~ sourcename, ~ vintage_freq, ~ vintage_start,
	 
	)


local({
	
	external_forecast_values
	
})

	
if (RESET_SQL) dbExecute(db, 'DROP TABLE IF EXISTS external_import_sources CASCADE')
if (!'external_import_sources' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
	dbExecute(
		db,
		'CREATE TABLE external_import_sources (
			id CONSTRAINT external_import_sources_pk PRIMARY KEY,
			sourcename VARCHAR(255) NOT NULL,
			release_freq CHAR(1) NOT NULL,
			release_start DATE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
	)
}
