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


# Forecast Tables ---------------------------------------------------------

## Get Table Structures ---------------------------------------------------------
forecasts = readxl::read_excel(
	file.path(EF_DIR, 'modules', 'create-sql-tables', 'forecast-tables.xlsx'), sheet = 'forecasts'
	)

forecast_variables = readxl::read_excel(
	file.path(EF_DIR, 'modules', 'create-sql-tables', 'forecast-tables.xlsx'), sheet = 'variables'
) 

forecast_hist_releases = readxl::read_excel(
	file.path(EF_DIR, 'modules', 'create-sql-tables', 'forecast-tables.xlsx'), sheet = 'releases'
) 

# Print all releases which map to no variable
anti_join(forecast_hist_releases, forecast_variables, by = c('id' = 'release'))
# Print all variables which map to no releases
anti_join(forecast_variables, forecast_hist_releases, by = c('release' = 'id'))

## Forecasts ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS forecasts CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE forecasts (
			id VARCHAR(50) CONSTRAINT forecasts_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT forecasts_fullname_uk UNIQUE NOT NULL,
			shortname VARCHAR(255) CONSTRAINT forecasts_shortname_uk UNIQUE NOT NULL,
			external BOOLEAN NOT NULL,
			update_freq CHAR(1) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
		)
	
	forecasts %>%
		create_insert_query(
			.,
			'forecasts',
			'ON CONFLICT ON CONSTRAINT forecasts_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    shortname=EXCLUDED.shortname,
		    external=EXCLUDED.external,
		    update_freq=EXCLUDED.update_freq'
			) %>%
		dbExecute(db, .)
})


## Variables ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS forecast_variables CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE forecast_variables (
			varname VARCHAR(50) CONSTRAINT forecast_variables_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT forecast_variables_fullname_uk UNIQUE NOT NULL,
			dispgroup VARCHAR(255) NOT NULL,
			disporder INT NULL,
			release VARCHAR(50) NOT NULL,
			units VARCHAR(255) NOT NULL,
			d1 VARCHAR(50) NOT NULL,
			d2 VARCHAR(50) NOT NULL,
			hist_source VARCHAR(255) NOT NULL,
			hist_source_key VARCHAR(255) NOT NULL,
			hist_source_freq CHAR(1) NOT NULL,
			hist_source_transform VARCHAR(255) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT forecast_variables_release_fk FOREIGN KEY (release)
				REFERENCES forecast_hist_releases (id) ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	forecast_variables %>%
		create_insert_query(
			.,
			'forecast_variables',
			'ON CONFLICT ON CONSTRAINT forecast_variables_pk DO UPDATE
		    SET
				fullname=EXCLUDED.fullname,
				dispgroup=EXCLUDED.dispgroup,
				disporder=EXCLUDED.disporder,
				release=EXCLUDED.release,
				units=EXCLUDED.units,
				d1=EXCLUDED.d1,
				d2=EXCLUDED.d2,
				hist_source=EXCLUDED.hist_source,
				hist_source_key=EXCLUDED.hist_source_key,
				hist_source_freq=EXCLUDED.hist_source_freq,
				hist_source_transform=EXCLUDED.hist_source_transform'
			) %>%
		dbExecute(db, .)
})

## Variable Hist Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS forecast_hist_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE forecast_hist_values (
			vdate DATE NOT NULL,
			form VARCHAR(50) NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (vdate, form, freq, varname, date),
			CONSTRAINT forecast_hist_values_fk FOREIGN KEY (varname)
				REFERENCES forecast_variables (varname) ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'forecast_hist_values\',
			time_column_name => \'vdate\'
			);
		')
})

## Variable Hist Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS forecast_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE forecast_values (
			forecast VARCHAR(50) NOT NULL,
			vdate DATE NOT NULL,
			form VARCHAR(50) NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (forecast, vdate, form, freq, varname, date),
			CONSTRAINT forecast_values_varname_fk FOREIGN KEY (varname)
				REFERENCES forecast_variables (varname) ON DELETE CASCADE ON UPDATE CASCADE,
			CONSTRAINT forecast_values_forecast_fk FOREIGN KEY (forecast)
				REFERENCES forecasts (id) ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'forecast_values\',
			time_column_name => \'vdate\'
			);
		')
})

# External Import Tables ---------------------------------------------------------

## Get Table Structures ---------------------------------------------------------
external_import_variables = read_csv(
	file.path(EF_DIR, 'modules', 'external-import', 'external-import-variables.csv'),
	col_types = 'ccccc'
	)

## Variables ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS external_import_variables CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE external_import_variables (
			varname VARCHAR(50) CONSTRAINT external_import_variables_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT external_import_variables_fullname_uk UNIQUE NOT NULL,
			hist_source VARCHAR(255) NOT NULL,
			hist_source_key VARCHAR(255),
			hist_source_freq CHAR(1),
			hist_source_transform VARCHAR(50),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
		)
	
	external_import_variables %>%
		create_insert_query(
			.,
			'external_import_variables',
			'ON CONFLICT ON CONSTRAINT external_import_variables_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    hist_source=EXCLUDED.hist_source,
		    hist_source_key=EXCLUDED.hist_source_key,
		    hist_source_freq=EXCLUDED.hist_source_freq,
		    hist_source_transform=EXCLUDED.hist_source_transform'
		) %>%
		dbExecute(db, .)
})


# Interest Rate Model ---------------------------------------------------------

## Get Table ---------------------------------------------------------
interest_rate_model_variables = read_csv(
	file.path(EF_DIR, 'modules', 'interest-rate-model', 'interest-rate-model-variables.csv'),
	col_types = 'ccccc'
)


## Variables ---------------------------------------------------------
local({

	dbExecute(db, 'DROP TABLE IF EXISTS interest_rate_model_variables CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE interest_rate_model_variables (
			varname VARCHAR(50) CONSTRAINT interest_rate_model_variables_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT interest_rate_model_variables_fullname_uk UNIQUE NOT NULL,
			hist_source VARCHAR(255) NOT NULL,
			hist_source_key VARCHAR(255),
			hist_source_freq CHAR(1),
			hist_source_transform VARCHAR(50),
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
	)
	
	interest_rate_model_variables %>%
		create_insert_query(
			.,
			'interest_rate_model_variables',
			'ON CONFLICT ON CONSTRAINT interest_rate_model_variables_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    hist_source=EXCLUDED.hist_source,
		    hist_source_key=EXCLUDED.hist_source_key,
		    hist_source_freq=EXCLUDED.hist_source_freq,
		    hist_source_transform=EXCLUDED.hist_source_transform'
		) %>%
		dbExecute(db, .)
})

## Input Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS interest_rate_model_input_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE interest_rate_model_input_values (
			vdate DATE NOT NULL,
			form VARCHAR(50) NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (vdate, form, freq, varname, date),
			CONSTRAINT interest_rate_model_input_values_varname_fk FOREIGN KEY (varname)
				REFERENCES interest_rate_model_variables (varname)
				ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'interest_rate_model_input_values\',
			time_column_name => \'vdate\'
			);
		')
})

# Nowcast ---------------------------------------------------------

## Get Table ---------------------------------------------------------
nowcast_model_variables = readxl::read_excel(
	file.path(EF_DIR, 'modules', 'nowcast-model', 'nowcast-model-variables.xlsx'), sheet = 'variables'
	)

nowcast_model_input_releases = readxl::read_excel(
	file.path(EF_DIR, 'modules', 'nowcast-model', 'nowcast-model-variables.xlsx'), sheet = 'releases'
	)

# Print all releases which map to no variable
anti_join(nowcast_model_input_releases, nowcast_model_variables, by = c('id' = 'release'))
# Print all variables which map to no releases
anti_join(nowcast_model_variables, nowcast_model_input_releases, by = c('release' = 'id'))


## Releases ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS nowcast_model_input_releases CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE nowcast_model_input_releases (
			id VARCHAR(50) CONSTRAINT nowcast_model_input_releases_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT nowcast_model_input_releases_fullname_uk UNIQUE NOT NULL,
			url VARCHAR(255) NULL,
			vintage_update VARCHAR(255) NOT NULL,
			source VARCHAR(255) NULL,
			source_key VARCHAR(255) NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			)'
	)
	
	nowcast_model_input_releases %>%
		create_insert_query(
			.,
			'nowcast_model_input_releases',
			'ON CONFLICT ON CONSTRAINT nowcast_model_input_releases_pk DO UPDATE
		    SET
		    fullname=EXCLUDED.fullname,
		    url=EXCLUDED.url,
		    vintage_update=EXCLUDED.vintage_update,
		    source=EXCLUDED.source,
				source_key=EXCLUDED.source_key'
		) %>%
		dbExecute(db, .)
})

## Release Dates ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS nowcast_model_input_release_dates CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE nowcast_model_input_release_dates (
			release VARCHAR(255) NOT NULL,
			date DATE NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (release, date),
			CONSTRAINT nowcast_model_input_release_dates_fk FOREIGN KEY (release)
				REFERENCES nowcast_model_input_releases (id) ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'nowcast_model_input_release_dates\',
			time_column_name => \'date\'
			);
		')
})

## Variables ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS nowcast_model_variables CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE nowcast_model_variables (
			varname VARCHAR(50) CONSTRAINT nowcast_model_variables_pk PRIMARY KEY,
			fullname VARCHAR(255) CONSTRAINT nowcast_model_variables_fullname_uk UNIQUE NOT NULL,
			dispgroup VARCHAR(255) NOT NULL,
			disporder INT NULL,
			release VARCHAR(50) NOT NULL,
			units VARCHAR(255) NOT NULL,
			st VARCHAR(50) NOT NULL,
			d1 VARCHAR(50) NOT NULL,
			d2 VARCHAR(50) NOT NULL,
			hist_source VARCHAR(255) NOT NULL,
			hist_source_key VARCHAR(255) NOT NULL,
			hist_source_freq CHAR(1) NOT NULL,
			hist_source_transform VARCHAR(255) NOT NULL,
			nc_dfm_input BOOLEAN NULL,
			nc_method VARCHAR(255) NULL,
			nc_input_reason VARCHAR(255) NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			CONSTRAINT nowcast_model_variables_release_fk FOREIGN KEY (release)
				REFERENCES nowcast_model_input_releases (id) ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	nowcast_model_variables %>%
		create_insert_query(
			.,
			'nowcast_model_variables',
			'ON CONFLICT ON CONSTRAINT nowcast_model_variables_pk DO UPDATE
		    SET
				fullname=EXCLUDED.fullname,
				dispgroup=EXCLUDED.dispgroup,
				disporder=EXCLUDED.disporder,
				release=EXCLUDED.release,
				units=EXCLUDED.units,
				st=EXCLUDED.st,
				d1=EXCLUDED.d1,
				d2=EXCLUDED.d2,
				hist_source=EXCLUDED.hist_source,
				hist_source_key=EXCLUDED.hist_source_key,
				hist_source_freq=EXCLUDED.hist_source_freq,
				hist_source_transform=EXCLUDED.hist_source_transform,
				nc_dfm_input=EXCLUDED.nc_dfm_input,
				nc_method=EXCLUDED.nc_method,
				nc_input_reason=EXCLUDED.nc_input_reason'
		) %>%
		dbExecute(db, .)
})

## Input Values ---------------------------------------------------------
local({
	
	dbExecute(db, 'DROP TABLE IF EXISTS nowcast_model_input_values CASCADE')
	
	dbExecute(
		db,
		'CREATE TABLE nowcast_model_input_values (
			vdate DATE NOT NULL,
			form VARCHAR(50) NOT NULL,
			freq CHAR(1) NOT NULL,
			varname VARCHAR(50) NOT NULL,
			date DATE NOT NULL,
			value NUMERIC(20, 4) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			PRIMARY KEY (vdate, form, freq, varname, date),
			CONSTRAINT nowcast_model_input_values_varname_fk FOREIGN KEY (varname)
				REFERENCES nowcast_model_variables (varname) ON DELETE CASCADE ON UPDATE CASCADE
			)'
	)
	
	dbExecute(
		db,
		'SELECT create_hypertable(
			relation => \'nowcast_model_input_values\',
			time_column_name => \'vdate\'
			);
		')
})
