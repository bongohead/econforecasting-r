CREATE MATERIALIZED  VIEW public.forecast_values_v2_all AS (
	-- Get unique mdate
	WITH t0 AS (
		SELECT
			forecast, vdate, form, freq, varname, date,
			first(value, mdate) AS value
		FROM forecast_values_v2
		GROUP BY forecast, vdate, form, freq, varname, date
	)
	-- Pivot out d1 and d2
	SELECT
		t1.forecast, t1.vdate, t1.freq, t1.varname, t1.date,
		t1.value AS d1, t2.value AS d2
	FROM t0 t1
	LEFT JOIN t0 t2 ON
		t1.forecast = t2.forecast
		AND t1.vdate = t2.vdate
		AND t1.freq = t2.freq
		AND t1.varname = t2.varname
		AND t1.date = t2.date
		AND t1.form = 'd1'
		AND t2.form = 'd2'
);

CREATE UNIQUE INDEX forecast_values_v2_all_uk ON forecast_values_v2_all (forecast, vdate, freq, varname, date);
CREATE INDEX forecast_values_v2_all_ix_forecast ON forecast_values_v2_all (forecast);
CREATE INDEX forecast_values_v2_all_ix_varname ON forecast_values_v2_all (varname);

