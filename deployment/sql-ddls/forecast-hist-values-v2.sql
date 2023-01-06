CREATE TABLE forecast_hist_values_v2 (
	vdate DATE NOT NULL,
	form VARCHAR(50) NOT NULL,
	freq CHAR(1) NOT NULL,
	varname VARCHAR(50) NOT NULL,
	date DATE NOT NULL,
	value NUMERIC(20, 4) NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT forecast_hist_values_v2_pk PRIMARY KEY (vdate, form, freq, varname, date),
	CONSTRAINT forecast_hist_values_v2_varname_fk FOREIGN KEY (varname) REFERENCES forecast_variables (varname) ON DELETE CASCADE ON UPDATE CASCADE
);

SELECT create_hypertable('forecast_hist_values_v2', 'date');
CREATE INDEX ON forecast_hist_values_v2 (varname);
CREATE INDEX ON forecast_hist_values_v2 (freq);
CREATE INDEX ON forecast_hist_values_v2 (date);
CREATE INDEX ON forecast_hist_values_v2 (form);
