# Initialize ----------------------------------------------------------
## Set Constants ----------------------------------------------------------
JOB_NAME = 'create-sql-tables'
EF_DIR = Sys.getenv('EF_DIR')

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
interest_rate_model_variables = read_csv(
	file.path(EF_DIR, 'modules', 'interest-rate-model', 'interest-rate-model-variables.csv'),
	col_types = 'ccclccc'
	)

# SQL ---------------------------------------------------------

## Variables ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS interest_rate_model_variables CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE interest_rate_model_variables (
			varname VARCHAR(50) CONSTRAINT interest_rate_model_variables_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT interest_rate_model_variables_fullname_uk UNIQUE NOT NULL,
			units VARCHAR(50) NOT NULL,
			sa BOOLEAN NOT NULL,
			hist_source VARCHAR(50) NOT NULL,
			hist_source_key VARCHAR(50),
			hist_source_freq CHAR(1),
			hist_source_transform VARCHAR(50),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
		)
	
	interest_rate_model_variables %>%
		transmute(
			.,
			varname, fullname, units, sa,
			hist_source, hist_source_key, hist_source_freq, hist_source_transform
			) %>%
		create_insert_query(
			.,
			'interest_rate_model_variables',
			'ON CONFLICT ON CONSTRAINT interest_rate_model_variables_pk DO UPDATE
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

## Hist Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS interest_rate_model_hist_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE interest_rate_model_hist_values (
			sourcename VARCHAR(50) NOT NULL,
			vdate DATE NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (sourcename, vdate, freq, varname, date),
			CONSTRAINT interest_rate_model_hist_values_varname_fk FOREIGN KEY (varname)
				REFERENCES interest_rate_model_variables (varname)
				ON DELETE CASCADE ON UPDATE CASCADE
			)'
		)

	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'interest_rate_model_hist_values\',
			time_column_name => \'vdate\'
			);
		')
})

## Forecast Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS interest_rate_model_forecast_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE interest_rate_model_forecast_values (
			vdate DATE NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (vdate, freq, varname, date),
			CONSTRAINT interest_rate_model_forecast_values_varnames_fk FOREIGN KEY (varname)
				REFERENCES interest_rate_model_variables (varname)
				ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'interest_rate_model_forecast_values\',
			time_column_name => \'vdate\'
			);
		')
})
