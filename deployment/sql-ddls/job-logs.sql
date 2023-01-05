DROP TABLE IF EXISTS public.job_logs;
CREATE TABLE public.job_logs (
	logname varchar(50) NOT NULL,
	module varchar(50) NOT NULL,
	log_date date NOT NULL,
	log_group varchar(50) NOT NULL,
	log_info json NOT NULL,
	log_dttm timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT job_logs_pk PRIMARY KEY (logname, module, log_date, log_group)
	);
