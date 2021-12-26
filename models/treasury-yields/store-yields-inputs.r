#'  Stores values & maps for baseline yields forecasts


# Load Libs ----------------------------------------------------------
library(tidyverse)
library(httr)
library(DBI)
library(econforecasting)

# Define Params ----------------------------------------------------------
rates_fcast_inputs = tribble(
	~ varname, ~ fullname, ~ source, ~ source_key,
	'ffr', 'Effective Federal Funds Rate', 'FRED', 'EFFR',
	'sofr', 'Secured Overnight Financing Rate', 'FRED', 'SOFR',
	'bsby', 'Bloomberg Short Term Bank Yield (Overnight Rate)', 'BLOOM', NA,
	't01m', 'Treasury Yield 1 Month', 'FRED', 'DGS1MO',
	't03m', 'Treasury Yield 3 Months', 'FRED', 'DGS3MO',
	't06m', 'Treasury Yield 6 Months', 'FRED', 'DGS6MO',
	't01y', 'Treasury Yield 1 Year', 'FRED', 'DGS1',
	't02y', 'Treasury Yield 2 Year', 'FRED', 'DGS2',
	't05y', 'Treasury Yield 5 Year', 'FRED', 'DGS5',
	't07y', 'Treasury Yield 7 Year', 'FRED', 'DGS7',
	't10y', 'Treasury Yield 10 Year', 'FRED', 'DGS10',
	't20y', 'Treasury Yield 20 Year', 'FRED', 'DGS20',
	't30y', 'Treasury Yield 30 Year', 'FRED', 'DGS30'
)


rates_fcast_outputs = tribble(
	't01m', 'Treasury Yield 1 Month',
	't03m', 'Treasury Yield 3 Month',
	'r01m', 'Real Treasury Yield 1 Month',
	'r03m', 'Real Treasury Yield 3 Month',
	
	)

# Send to SQL  -------------------------------------------------

## 1. Reset   -------------------------------------------------
local({

	if (RESET_SQL) {

		dbExecute(db, 'DROP TABLE IF EXISTS external_forecast_values CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS external_forecast_names CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS historical_values CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS variable_params CASCADE')
		dbExecute(db, 'DROP TABLE IF EXISTS variable_releases CASCADE')
	}

})


## 1. Releases  -------------------------------------------------
local({

	if (!'variable_releases' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(
			db,
			'CREATE TABLE variable_releases (
				relkey VARCHAR(255) CONSTRAINT relkey_pk PRIMARY KEY,
				relname VARCHAR(255) CONSTRAINT relname_uk UNIQUE NOT NULL,
				relsc VARCHAR(255) NOT NULL,
				relsckey VARCHAR(255) ,
				relurl VARCHAR(255),
				relnotes TEXT,
				n_varnames INTEGER,
				varnames JSON,
				n_dfm_varnames INTEGER,
				dfm_varnames JSON,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				)'
			)
	}

	releases_final %>%
		transmute(
			.,
			relkey, relname, relsc, relsckey , relurl, relnotes,
			n_varnames = ifelse(is.na(n_varnames), 0, n_varnames), varnames,
			n_dfm_varnames = ifelse(is.na(n_dfm_varnames), 0, n_dfm_varnames), dfm_varnames
			) %>%
		create_insert_query(
			.,
			'variable_releases',
			str_squish(
				'ON CONFLICT ON CONSTRAINT relkey_pk DO UPDATE
		    SET
		    relname=EXCLUDED.relname,
		    relsc=EXCLUDED.relsc,
		    relsckey=EXCLUDED.relsckey,
		    relurl=EXCLUDED.relurl,
		    relnotes=EXCLUDED.relnotes,
		    n_varnames=EXCLUDED.n_varnames,
		    varnames=EXCLUDED.varnames,
		    n_dfm_varnames=EXCLUDED.n_dfm_varnames,
		    dfm_varnames=EXCLUDED.dfm_varnames'
				)
			) %>%
		dbSendQuery(db, .)
})

## 2. Variables  -------------------------------------------------
local({

	if (!'variable_params' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(
			db,
			'CREATE TABLE variable_params (
				varname VARCHAR(255) CONSTRAINT varname_pk PRIMARY KEY,
				fullname VARCHAR(255) CONSTRAINT fullname_uk UNIQUE NOT NULL,
				category VARCHAR(255) NOT NULL,
				dispgroup VARCHAR(255),
				disprank INT,
				disptabs INT,
				disporder INT,
				source VARCHAR(255) NOT NULL,
				sckey VARCHAR(255),
				relkey VARCHAR(255) NOT NULL,
				units VARCHAR(255) NOT NULL,
				freq CHAR(1) NOT NULL,
				sa CHAR(1) NOT NULL,
				st VARCHAR(50) NOT NULL,
				st2 VARCHAR(50) NOT NULL,
				d1 VARCHAR(50) NOT NULL,
				d2 VARCHAR(50) NOT NULL,
				nc_dfm_input BOOLEAN,
				nc_method VARCHAR(50) NOT NULL,
				initial_forecast VARCHAR(50),
				core_structural VARCHAR(50) NOT NULL,
				core_endog_type VARCHAR(50),
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT relkey_fk FOREIGN KEY (relkey) REFERENCES variable_releases (relkey)
					ON DELETE CASCADE ON UPDATE CASCADE
				)'
			)
	}

	variable_params %>%
		transmute(
			.,
			varname, fullname, category, dispgroup, disprank, disptabs, disporder,
			source, sckey, relkey, units, freq,
			sa, st, st2, d1, d2, nc_dfm_input, nc_method, initial_forecast,
			core_structural, core_endog_type
			) %>%
		create_insert_query(
			.,
			'variable_params',
			str_squish(
				'ON CONFLICT ON CONSTRAINT varname_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    category=EXCLUDED.category,
		    dispgroup=EXCLUDED.dispgroup,
		    disprank=EXCLUDED.disprank,
		    disptabs=EXCLUDED.disptabs,
		    disporder=EXCLUDED.disporder,
		    source=EXCLUDED.source,
		    sckey=EXCLUDED.sckey,
		    relkey=EXCLUDED.relkey,
		    units=EXCLUDED.units,
		    freq=EXCLUDED.freq,
		    sa=EXCLUDED.sa,
		    st=EXCLUDED.st,
		    st2=EXCLUDED.st2,
		    d1=EXCLUDED.d1,
		    d2=EXCLUDED.d2,
		    nc_dfm_input=EXCLUDED.nc_dfm_input,
		    nc_method=EXCLUDED.nc_method,
		    initial_forecast=EXCLUDED.initial_forecast,
		    core_structural=EXCLUDED.core_structural,
		    core_endog_type=EXCLUDED.core_endog_type'
				)
			) %>%
		dbSendQuery(db, .)

})


## 3. External Forecast Names -------------------------------------------------
local({

	if (!'external_forecast_names' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {
		dbExecute(
			db,
			'CREATE TABLE external_forecast_names (
				tskey VARCHAR(10) CONSTRAINT external_forecast_names_pk PRIMARY KEY,
				forecast_type VARCHAR(255),
				shortname VARCHAR(100) NOT NULL,
				fullname VARCHAR(255) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
				)'
			)
		dbExecute(db, 'CREATE INDEX external_forecast_names_ix_fctype ON external_forecast_names (forecast_type)')
	}

	external_forecast_names =
		tribble(
			~ tskey, ~ forecast_type, ~ shortname, ~ fullname,
			# 1
			'atl', 'quant', 'Atlanta Fed', 'Atlanta Fed GDPNow Model',
			# 2
			'stl', 'qual', 'St. Louis Fed', 'St. Louis Fed Economic News Model',
			# 3
			'nyf', 'qual', 'NY Fed', 'New York Fed Staff Nowcast',
			# 4
			'spf', 'qual', 'Survey of Professional Forecasters', 'Survey of Professional Forecasters',
			# 5
			'wsj', 'qual', 'WSJ Consensus', 'Wall Street Journal Consensus Forecast',
			'fnm', 'qual', 'Fannie Mae', 'Fannie Mae Forecast',
			'wfc', 'qual', 'Wells Fargo', 'Wells Fargo Forecast',
			'gsu', 'qual', 'GSU', 'Georgia State University Forecast',
			'sp', 'qual', 'S&P', 'S&P Global Ratings',
			'ucla', 'qual', 'UCLA Anderson', 'UCLA Anderson Forecast',
			'gs', 'qual', 'Goldman Sachs', 'Goldman Sachs Forecast',
			'ms', 'qual', 'Morgan Stanley', 'Morgan Stanley',
			# 6
			'cbo', 'quant', 'CBO', 'Congressional Budget Office Projections',
			# 7
			'cle', 'fut', 'Futures-Implied Inflation Rates', 'Futures-Implied Expected Inflation Model',
			# 8
			'cme', 'fut', 'Futures-Implied Interest Rates', 'Futures-Implied Expected Benchmark Rates Model',
			# 9
			'dns', 'fut', 'Futures-Implied Treasury Yield', 'Futures-Implied Expected Treasury Yields Model'
			)

	ext_final %>% group_by(., fcname) %>% summarize(., n = n())

	external_forecast_names %>%
		create_insert_query(
			.,
			'external_forecast_names',
			str_squish(
				'ON CONFLICT ON CONSTRAINT external_forecast_names_pk DO UPDATE
		    SET
		    forecast_type=EXCLUDED.forecast_type,
		    shortname=EXCLUDED.shortname,
		    fullname=EXCLUDED.fullname'
				)
			) %>%
		dbSendQuery(db, .)
})


## 4. Forecast Values ------------------------------------------------
local({

	if (!'external_forecast_values' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {

		dbExecute(
			db,
			'CREATE TABLE external_forecast_values (
				tskey VARCHAR(10) NOT NULL,
				vdate DATE NOT NULL,
				freq CHAR(1) NOT NULL,
				transform VARCHAR(255) NOT NULL,
				varname VARCHAR(255) NOT NULL,
				date DATE NOT NULL,
				value NUMERIC(20, 4) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT external_forecast_values_pk PRIMARY KEY (tskey, vdate, freq, transform, varname, date),
				CONSTRAINT external_forecast_values_fk FOREIGN KEY (tskey) REFERENCES external_forecast_names (tskey)
					ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT ext_tsvalues_varname_fk FOREIGN KEY (varname) REFERENCES variable_params (varname)
					ON DELETE CASCADE ON UPDATE CASCADE
				)'
			)

		dbExecute(db, '
			SELECT create_hypertable(
				relation => \'external_forecast_values\',
				time_column_name => \'vdate\'
				);
			')
	}

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_forecast_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		ext_final %>%
		transmute(., tskey = fcname, vdate, freq, transform, varname, date, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_by(., split) %>%
		group_split(., .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'external_forecast_values',
				str_squish(
					'ON CONFLICT (tskey, vdate, freq, transform, varname, date) DO UPDATE
			    SET value=EXCLUDED.value'
					)
				) %>%
				DBI::dbExecute(db, .)
			) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}


	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL:')
	print(sum(unlist(sql_result)))

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM external_forecast_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)

})

## 5. Historical Values ------------------------------------------------
local({

	if (!'historical_values' %in% dbGetQuery(db, 'SELECT * FROM pg_catalog.pg_tables')$tablename) {

		dbExecute(
			db,
			'CREATE TABLE historical_values (
				varname VARCHAR(255) NOT NULL,
				transform VARCHAR(255) NOT NULL,
				freq CHAR(1) NOT NULL,
				date DATE NOT NULL,
				vdate DATE NOT NULL,
				value NUMERIC(20, 4) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT historical_values_pk PRIMARY KEY (varname, transform, freq, date, vdate),
				CONSTRAINT historical_values_varname_fk FOREIGN KEY (varname) REFERENCES variable_params (varname)
					ON DELETE CASCADE ON UPDATE CASCADE
				)'
			)

		dbExecute(db, '
			SELECT create_hypertable(
				relation => \'historical_values\',
				time_column_name => \'vdate\'
				);
			')
	}

	initial_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM historical_values')$count)
	message('***** Initial Count: ', initial_count)

	sql_result =
		hist_final %>%
		transmute(., varname, transform, freq, date, vdate, value) %>%
		mutate(., split = ceiling((1:nrow(.))/5000)) %>%
		group_by(., split) %>%
		group_split(., .keep = FALSE) %>%
		sapply(., function(x)
			create_insert_query(
				x,
				'historical_values',
				str_squish(
					'ON CONFLICT (varname, transform, freq, date, vdate) DO UPDATE
			    SET value=EXCLUDED.value'
					)
				) %>%
				DBI::dbExecute(db, .)
			) %>%
		{if (any(is.null(.))) stop('SQL Error!') else sum(.)}


	if (any(is.null(unlist(sql_result)))) stop('Error with one or more SQL queries')
	sql_result %>% imap(., function(x, i) paste0(i, ': ', x)) %>% paste0(., collapse = '\n') %>% cat(.)
	message('***** Data Sent to SQL:')
	print(sum(unlist(sql_result)))

	final_count = as.numeric(dbGetQuery(db, 'SELECT COUNT(*) AS count FROM historical_values')$count)
	message('***** Initial Count: ', final_count)
	message('***** Rows Added: ', final_count - initial_count)

})
