## ----------------------------------------------------------------------
# General purpose
library(tidyverse) # General
library(data.table) # General
library(devtools) # General
library(lubridate) # Dates
library(glue) # String Interpolation
# Data parse/import
library(jsonlite) # JSON Parser
library(rvest) # HTML Parser
library(httr) # CURL Interface
# Visualization
library(ggpmisc) # Tables
library(cowplot) # Join ggplots
# SQL/Apache Spark
library(DBI) # SQL Interface
# My package
devtools::load_all(path = PACKAGE_DIR)
devtools::document(PACKAGE_DIR)

# Set working directory
setwd(DIR)

# Read constants
source(file.path(INPUT_DIR, 'constants.r'))


## ----------------------------------------------------------------------
local({
	
	rds = readRDS(paste0(OUTPUT_DIR, '/[', VINTAGE_DATE, '] m4.rds'))

	p <<- rds$p
	m <<- rds$m
	h <<- rds$h
})


## ----------------------------------------------------------------------
local({
	
	db = dbConnect(
		RPostgres::Postgres(),
		dbname = CONST$DB_DATABASE,
		host = CONST$DB_SERVER,
		port = 5432,
		user = CONST$DB_USERNAME,
		password = CONST$DB_PASSWORD
		)
	
	# if (RESET_ALL == TRUE) {
	# 	
	# 	DBI::dbGetQuery(conn, 'TRUNCATE csm_releases')
	# 	DBI::dbGetQuery(conn, 'TRUNCATE csm_values')
	# 	DBI::dbGetQuery(conn, 'TRUNCATE csm_history')
	# }
	
	db <<- db
})


## ----------------------------------------------------------------------
local({
	
	if (RESET_ALL) {
		DBI::dbGetQuery(db, 'DROP TABLE IF EXISTS csm_releases CASCADE')
		DBI::dbGetQuery(db, '
			CREATE TABLE csm_releases (
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
			)
		')
	}
	
	econforecasting::createInsertQuery(
	    p$releasesDf %>%
	    	transmute(
	    		.,
	    		relkey, relname, relsc, relsckey , relurl, relnotes,
	    		n_varnames = ifelse(is.na(n_varnames), 0, n_varnames), varnames,
	    		n_dfm_varnames = ifelse(is.na(n_dfm_varnames), 0, n_dfm_varnames), dfm_varnames
	    	),
	    'csm_releases',
	    str_squish('ON CONFLICT ON CONSTRAINT relkey_pk DO UPDATE
	    SET
	    relname=EXCLUDED.relname,
	    relsc=EXCLUDED.relsc,
	    relsckey=EXCLUDED.relsckey,
	    relurl=EXCLUDED.relurl,
	    relnotes=EXCLUDED.relnotes,
	    n_varnames=EXCLUDED.n_varnames,
	    varnames=EXCLUDED.varnames,
	    n_dfm_varnames=EXCLUDED.n_dfm_varnames,
	    dfm_varnames=EXCLUDED.dfm_varnames
	    ')) %>%
		DBI::dbSendQuery(db, .)

	
})


## ----------------------------------------------------------------------
local({
	
	if (RESET_ALL) {
		DBI::dbGetQuery(db, 'DROP TABLE IF EXISTS csm_params CASCADE')
		DBI::dbGetQuery(db, '
			CREATE TABLE csm_params (
				varname VARCHAR(255) CONSTRAINT varname_pk PRIMARY KEY,
				fullname VARCHAR(255) CONSTRAINT fullname_uk UNIQUE NOT NULL,
				category VARCHAR(255) NOT NULL,
				dispgroup VARCHAR(255),
				disporder INT,
				source VARCHAR(255) NOT NULL,
				sckey VARCHAR(255),
				relkey VARCHAR(255) NOT NULL,
				units VARCHAR(255) NOT NULL,
				freq CHAR(1) NOT NULL,
				append_eom_with_currentval CHAR(1),
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
				CONSTRAINT relkey_fk FOREIGN KEY (relkey) REFERENCES csm_releases (relkey)
					ON DELETE CASCADE ON UPDATE CASCADE
			)
		')
	}
	
	econforecasting::createInsertQuery(
	    p$variablesDf %>%
	    	transmute(
	    		.,
	    		varname, fullname, category, dispgroup, disporder,
	    		source, sckey, relkey, units, freq, append_eom_with_currentval,
	    		sa, st, st2, d1, d2, nc_dfm_input, nc_method, initial_forecast,
	    		core_structural, core_endog_type
	    	),
	    'csm_params',
	    ) %>%
		DBI::dbSendQuery(db, .)

})


## ----------------------------------------------------------------------
local({
	
	if (RESET_ALL) {
		DBI::dbGetQuery(db, 'DROP TABLE IF EXISTS csm_tstypes CASCADE')
		
		# tstypes 'hist', 'forecast', 'nc'
		DBI::dbGetQuery(db, '
			CREATE TABLE csm_tstypes (
				tskey VARCHAR(10) CONSTRAINT tskey_pk PRIMARY KEY,
				tstype CHAR(1) NOT NULL, --h or fc
				fctype VARCHAR(255), -- fc type
				shortname VARCHAR(100) NOT NULL,
				fullname VARCHAR(255) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
			);
		')
		
		DBI::dbGetQuery(db, 'CREATE INDEX tstype_ix ON csm_tstypes (tstype);')
		DBI::dbGetQuery(db, 'CREATE INDEX fctype_ix ON csm_tstypes (fctype);')
	}
		
	tsTypes = tribble(
		~ tskey, ~ tstype, ~ fctype, ~ shortname, ~ fullname,
		'hist', 'h', NA, 'Historical Data', 'Historical Data',
		'baseline', 'f', 'csm', 'Baseline Model', 'CMEFI Baseline Economic Forecast',
		'downside', 'f', 'csm', 'Downside Model', 'CMEFI Downside Economic Forecast',
		'nc', 'f', 'nc', 'Nowcast Model', 'CMEFI Macroeconomic Nowcast',
		'atl', 'f', 'ext', 'Atlanta Fed', 'Atlanta Fed GDPNow Model',
		'stl', 'f', 'ext', 'St. Louis Fed', 'St. Louis Fed Economic News Model',
		'nyf', 'f', 'ext', 'NY Fed', 'New York Fed Staff Nowcast',
		'spf', 'f', 'ext', 'Survey of Professional Forecasters', 'Survey of Professional Forecasters',
		'wsj', 'f', 'ext', 'WSJ Consensus', 'Wall Street Journal Consensus Forecast',
		'fnm', 'f', 'ext', 'Fannie Mae', 'Fannie Mae Forecast',
		'gsu', 'f', 'ext', 'GSU', 'Georgia State University Forecast',
		'gsc', 'f', 'ext', 'Goldman Sachs', 'Goldman Sachs Forecast',
		'spg', 'f', 'ext', 'S&P Global', 'S&P Global Ratings',
		'ucl', 'f', 'ext', 'UCLA Anderson', 'UCLA Anderson Forecast',
		'wfc', 'f', 'ext', 'Wells Fargo', 'Wells Fargo Forecast',

		'cbo', 'f', 'ext', 'CBO', 'Congressional Budget Office Projections',
		'cle', 'f', 'fut', 'Futures-Implied Inflation Rates', 'Futures-Implied Expected Inflation Model',
		'cme', 'f', 'fut', 'Futures-Implied Interest Rates', 'Futures-Implied Expected Benchmark Rates Model',
		'dns', 'f', 'fut', 'Futures-Implied Treasury Yield', 'Futures-Implied Expected Treasury Yields Model'
		)

	econforecasting::createInsertQuery(tsTypes, 'csm_tstypes') %>%
		DBI::dbSendQuery(db, .)


})


## ----------------------------------------------------------------------
local({
	
	if (RESET_ALL) {
		DBI::dbGetQuery(db, 'DROP TABLE IF EXISTS csm_tsvalues')
		
		# tstypes 'hist', 'forecast', 'nc'
		DBI::dbGetQuery(db, '
			CREATE TABLE csm_tsvalues (
				tskey VARCHAR(10) NOT NULL,
				vdate DATE NOT NULL,
				freq CHAR(1) NOT NULL,
				form VARCHAR(5) NOT NULL,
				varname VARCHAR(255) NOT NULL,
				date DATE NOT NULL,
				value NUMERIC(20, 4) NOT NULL,
				created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
				CONSTRAINT tsvalues_pk PRIMARY KEY (tskey, vdate, freq, form, varname, date),
				CONSTRAINT tskey_fk FOREIGN KEY (tskey) REFERENCES csm_tstypes (tskey)
					ON DELETE CASCADE ON UPDATE CASCADE,
				CONSTRAINT varname_fk FOREIGN KEY (varname) REFERENCES csm_params (varname)
					ON DELETE CASCADE ON UPDATE CASCADE
			);
		')
	}
	
	onConflict = str_squish('ON CONFLICT ON CONSTRAINT tsvalues_pk DO UPDATE
	    SET
	    tskey=EXCLUDED.tskey,
	    vdate=EXCLUDED.vdate,
	    freq=EXCLUDED.freq,
	    form=EXCLUDED.form,
	    varname=EXCLUDED.varname,
	    date=EXCLUDED.date
	    ')
	
	resVals = list()
	
	# Hist
	resVals$hist =
		econforecasting::createInsertQuery(
			h$flat %>%
	    	transmute(., tskey = 'hist', vdate = VINTAGE_DATE, freq, form, varname, date, value),
		    'csm_tsvalues',
		    onConflict
			) %>%
		DBI::dbGetQuery(db, .)

	# CSM
	resVals$csm =
		econforecasting::createInsertQuery(
			m$csm$predFlat %>%
	    		transmute(., tskey = scenarioname, vdate = VINTAGE_DATE, freq, form, varname, date, value),
		    'csm_tsvalues',
		    onConflict
			) %>%
		DBI::dbGetQuery(db, .)

	# NC
	resVals$nc =
		econforecasting::createInsertQuery(
			m$ncpredFlat %>%
				transmute(., tskey = 'nc', vdate, freq, form, varname, date, value),
			'csm_tsvalues',
			onConflict
			) %>%
		DBI::dbGetQuery(db, .)
	
	# External Forecasts
	resVals$ext =
		econforecasting::createInsertQuery(
			dplyr::bind_rows(m$ext$sources) %>%
				transmute(., tskey = fcname, vdate, freq, form, varname, date, value),
	    	'csm_tsvalues',
	    	onConflict
	    	) %>%
		DBI::dbGetQuery(db, .)
})

