CREATE MATERIALIZED VIEW public.forecast_hist_values_v2_latest AS (
	WITH t0 AS (
		-- Pivot out d1 and d2
		SELECT
			t1.vdate, t1.freq, t1.varname, t1.date,
			t1.value AS d1, t2.value AS d2
		FROM (SELECT * FROM forecast_hist_values_v2 WHERE form = 'd1') t1
		LEFT JOIN (SELECT * FROM forecast_hist_values_v2 WHERE form = 'd2') t2 ON (
			t1.vdate = t2.vdate
			AND t1.freq = t2.freq
			AND t1.varname = t2.varname
			AND t1.date = t2.date
		)
	)
	-- Now aggregate out vdates to get the latest freq x date x varname combo
	SELECT
		freq, date, varname, MAX(vdate) as vdate, last(d1, vdate) as d1, last(d2, vdate) AS d2
	FROM t0
	GROUP BY varname, freq, date
	ORDER BY varname, freq, date
);


CREATE UNIQUE INDEX forecast_hist_values_v2_uk ON forecast_hist_values_v2_latest (vdate, freq, varname, date);
CREATE INDEX forecast_hist_values_v2_latest_ix_vdate ON forecast_hist_values_v2_latest (vdate);
CREATE INDEX forecast_hist_values_v2_latest_ix_freq ON forecast_hist_values_v2_latest (freq);
CREATE INDEX forecast_hist_values_v2_latest_ix_varname ON forecast_hist_values_v2_latest(varname);
CREATE INDEX forecast_hist_values_v2_latest_ix_date ON forecast_hist_values_v2_latest (date);
