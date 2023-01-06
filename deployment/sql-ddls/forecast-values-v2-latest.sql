CREATE MATERIALIZED  VIEW public.forecast_values_v2_latest AS (
	SELECT
	forecast, freq, varname, date, d1, d2, vdate
	FROM
	(
		-- Aggregates over dates to get the latest set of obs only
		SELECT forecast, freq, varname, vdate, date, d1, d2, MAX(vdate) OVER (partition by forecast, freq, varname) AS max_vdate
		FROM
		(
			-- Aggregate over vdates to get the last value for each date forecast
			SELECT
			forecast, freq, date, varname, MAX(vdate) as vdate, last(d1, vdate) as d1, last(d2, vdate) AS d2
			FROM forecast_values_v2_all
			GROUP BY forecast, varname, freq, date
			ORDER BY forecast, varname, freq, date
		) a
	) b
	WHERE max_vdate = vdate
	ORDER BY vdate, date
)

CREATE UNIQUE INDEX forecast_values_v2_latest_uk ON forecast_values_v2_latest (forecast, freq, varname, date);
CREATE INDEX forecast_values_v2_latest_ix_forecast ON forecast_values_v2_latest (forecast);
CREATE INDEX forecast_values_v2_latest_ix_varname ON forecast_values_v2_latest (varname);
CREATE INDEX forecast_values_v2_latest_ix_freq ON forecast_values_v2_latest (freq);
CREATE INDEX forecast_values_v2_latest_ix_date ON forecast_values_v2_latest (date);
