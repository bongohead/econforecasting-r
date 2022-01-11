#' Removed from rates-model-forecast.r on 1/11/22
#' Deprecated

local({
	
	message('Starting Quandl data scrape')
	quandl_data =
		purrr::map_dfr(1:24, function(j)
			read_csv(
				str_glue('https://www.quandl.com/api/v3/datasets/CHRIS/CME_FF{j}.csv?api_key={CONST$QUANDL_API_KEY}'),
				col_types = 'Ddddddddd'
			) %>%
				transmute(., vdate = Date, settle = Settle, j = j) %>%
				filter(., vdate >= as_date('2010-01-01'))
		) %>%
		transmute(
			.,
			varname = 'ffr',
			vdate,
			date = # Consider the forecasted period the vdate + j
				from_pretty_date(paste0(year(vdate), 'M', month(vdate)), 'm') %>%
				add_with_rollback(., months(j - 1), roll_to_first = TRUE),
			value = 100 - settle
		)
	
	if (RESET_SQL) dbExecute(db, 'DROP TABLE IF EXISTS rates_model_quandl')
	if (!'rates_model_quandl' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(db,
							'CREATE TABLE rates_model_quandl (
				varname VARCHAR(255),
				vdate DATE,
				date DATE,
				value NUMERIC (20, 4),
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT rates_model_quandl_pk PRIMARY KEY (varname, vdate, date)
				)'
		)
		dbExecute(db, 'SELECT create_hypertable(relation => \'rates_model_quandl\', time_column_name => \'vdate\')')
	}
	dbExecute(db, create_insert_query(
		quandl_data,
		'rates_model_quandl',
		'ON CONFLICT (varname, vdate, date) DO UPDATE SET value=EXCLUDED.value'
	))
})
