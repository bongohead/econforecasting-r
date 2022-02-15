# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'external-import-create-sql-tables'
EF_DIR = Sys.getenv('EF_DIR')

## Log Job ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Libs ----------------------------------------------------------
library(tidyverse)
library(httr)
library(DBI)
library(RPostgres)
library(econforecasting)
library(lubridate)

## Load Connection Info ----------------------------------------------------------
source(file.path(EF_DIR, 'model-inputs', 'constants.r'))
db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
	)

# Set Table Contents ---------------------------------------------------------

## Variables Table ---------------------------------------------------------
external_import_variables = read_csv(
	file.path(EF_DIR, 'modules', 'external-imports', 'external-import-variables.csv'),
	col_types = 'ccclccc'
	)

## Sources Table ---------------------------------------------------------
external_import_sources = tribble(
	~ sourcename, ~ fullname, ~ raw, ~ vintage_freq,
		'spf', 'Survey of Professional Forecasters', T, 'q',
		'einf', 'Expected Inflations Model', F, 'm'
	)


# SQL ---------------------------------------------------------

## Variables ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS external_import_variables CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE external_import_variables (
			varname VARCHAR(50) CONSTRAINT external_import_variables_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT external_import_variables_fullname_uk UNIQUE NOT NULL,
			units VARCHAR(50) NOT NULL,
			sa BOOLEAN NOT NULL,
			hist_source VARCHAR(50) NOT NULL,
			hist_source_key VARCHAR(50),
			hist_source_freq CHAR(1),
			hist_source_transform VARCHAR(50),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
		)
	
	external_import_variables %>%
		transmute(
			.,
			varname, fullname, units, sa,
			hist_source, hist_source_key, hist_source_freq, hist_source_transform
			) %>%
		create_insert_query(
			.,
			'external_import_variables',
			'ON CONFLICT ON CONSTRAINT external_import_variables_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    units=EXCLUDED.units,
		    sa=EXCLUDED.sa,
		    hist_source=EXCLUDED.hist_source,
		    hist_source_key=EXCLUDED.hist_source_key,
		    hist_source_freq=EXCLUDED.hist_source_freq,
		    hist_source_transform=EXCLUDED.hist_source_transform'
			) %>%
		dbExecute(db, .)
})


## Sources ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS external_import_sources CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE external_import_sources (
			sourcename VARCHAR(50) CONSTRAINT external_import_sources_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT external_import_sources_fullname_uk UNIQUE NOT NULL,
			raw BOOLEAN NOT NULL,
			vintage_freq CHAR(1) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
		)
	
	external_import_sources %>%
		transmute(
			.,
			sourcename, fullname, raw, vintage_freq
		) %>%
		create_insert_query(
			.,
			'external_import_sources',
			'ON CONFLICT ON CONSTRAINT external_import_sources_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    raw=EXCLUDED.raw,
		    vintage_freq=EXCLUDED.vintage_freq'
			) %>%
		dbExecute(db, .)
})

## Hist Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS external_import_hist_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE external_import_hist_values (
			vdate DATE NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (vdate, freq, varname, date),
			CONSTRAINT external_import_hist_values_varname_fk FOREIGN KEY (varname)
				REFERENCES external_import_variables (varname)
				ON DELETE CASCADE ON UPDATE CASCADE
			)'
		)

	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'external_import_hist_values\',
			time_column_name => \'vdate\'
			);
		')
})

## Forecast Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS external_import_forecast_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE external_import_forecast_values (
			sourcename VARCHAR(50) NOT NULL,
			vdate DATE NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (sourcename, vdate, freq, varname, date),
			CONSTRAINT external_import_forecast_values_varname_fk FOREIGN KEY (varname)
				REFERENCES external_import_variables (varname)
				ON DELETE CASCADE ON UPDATE CASCADE,
			CONSTRAINT external_import_forecast_values_sourcename_fk FOREIGN KEY (sourcename)
				REFERENCES external_import_sources (sourcename)
				ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'external_import_forecast_values\',
			time_column_name => \'vdate\'
			);
		')
})


## Import Notes ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS external_import_logs CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE external_import_logs (
			sourcename VARCHAR(50) NOT NULL,
			import_date DATE NOT NULL,
			rows_added INTEGER NOT NULL,
			PRIMARY KEY (sourcename, import_date),
			CONSTRAINT external_import_logs_sourcename_fk FOREIGN KEY (sourcename)
				REFERENCES external_import_sources (sourcename)
				ON DELETE CASCADE ON UPDATE CASCADE
		)'
	)
	
})