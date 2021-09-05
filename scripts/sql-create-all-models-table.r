DIR = 'D:/Onedrive/__Projects/econforecasting'
PACKAGE_DIR = 'D:/Onedrive/__Projects/econforecasting/r-package'
INPUT_DIR = 'D:/Onedrive/__Projects/econforecasting/model-inputs'
source(file.path(INPUT_DIR, 'constants.r'))

library(tidyverse)
library(DBI)
devtools::load_all(path = PACKAGE_DIR)
devtools::document(PACKAGE_DIR)


db = dbConnect(
	RPostgres::Postgres(),
	dbname = CONST$DB_DATABASE,
	host = CONST$DB_SERVER,
	port = 5432,
	user = CONST$DB_USERNAME,
	password = CONST$DB_PASSWORD
)



DBI::dbGetQuery(db, 'DROP TABLE IF EXISTS model_types')
DBI::dbGetQuery(db, '
		CREATE TABLE model_types (
			modelkey VARCHAR(10) CONSTRAINT modelkey_pk PRIMARY KEY,
			modelname VARCHAR(255) CONSTRAINT modelname_uk UNIQUE NOT NULL,
			date_created DATE NOT NULL DEFAULT CURRENT_DATE
		);
	')

econforecasting::createInsertQuery(
	tribble(
		~ modelkey, ~ modelname, ~ date_created
		'csm', 'External Forecasts', Sys.Date()
		),
	'model_types',
	str_squish('ON CONFLICT ON CONSTRAINT relkey_pk DO UPDATE
	    SET
	    modelname=EXCLUDED.modelname
	    ')
	) %>%
	DBI::dbSendQuery(db, .)


# DBI::dbGetQuery(db, 'DROP TABLE IF EXISTS model_runs')
# # tstypes 'hist', 'forecast', 'nc'
# DBI::dbGetQuery(db, '
# 		CREATE TABLE csm_tsvalues (
# 			id VARCHAR(10) NOT NULL,
# 			modelname VARCHAR(10) NOT NULL,
# 			freq CHAR(1) NOT NULL,
# 			form VARCHAR(5) NOT NULL,
# 			varname VARCHAR(255) NOT NULL,
# 			date DATE NOT NULL,
# 			value NUMERIC(20, 4) NOT NULL,
# 			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
# 			CONSTRAINT tsvalues_pk PRIMARY KEY (tskey, vdate, freq, form, varname, date),
# 			CONSTRAINT tskey_fk FOREIGN KEY (tskey) REFERENCES csm_tstypes (tskey)
# 				ON DELETE CASCADE ON UPDATE CASCADE,
# 			CONSTRAINT varname_fk FOREIGN KEY (varname) REFERENCES csm_params (varname)
# 				ON DELETE CASCADE ON UPDATE CASCADE
# 		);
# 	')
