CREATE TABLE public.forecast_values_v2 (
	mdate date not null,
	forecast varchar(50) NOT NULL,
	vdate date NOT NULL,
	form varchar(50) NOT NULL,
	freq bpchar(1) NOT NULL,
	varname varchar(50) NOT NULL,
	"date" date NOT NULL,
	value numeric(20, 4) NOT NULL,
	created_at timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT forecast_values_v2_pkey PRIMARY KEY (mdate, forecast, vdate, form, freq, varname, date),
	CONSTRAINT forecast_values_v2_forecast_fk FOREIGN KEY (forecast) REFERENCES public.forecasts(id) ON DELETE CASCADE ON UPDATE CASCADE,
	CONSTRAINT forecast_values_v2_varname_fk FOREIGN KEY (varname) REFERENCES public.forecast_variables(varname) ON DELETE CASCADE ON UPDATE CASCADE
);

SELECT create_hypertable('forecast_values_v2', 'mdate');
CREATE INDEX ON forecast_values_v2 (forecast, vdate, form, freq, varname, date);


-- Adding INDEX & hypertable reduces from 2000 ms -> 400 ms
EXPLAIN ANALYZE (
	SELECT
		forecast, vdate, form, freq, varname, "date", first(value, mdate)
	FROM forecast_values_v2
	GROUP BY forecast, vdate, form, freq, varname, "date"
)
