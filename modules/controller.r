# Load Libs ---------------------------------------------------------------
library(optparse)
library(jsonlite)
library(dotenv)
library(RPostgres)
library(DBI)

# Load Args ---------------------------------------------------------------
if (interactive() == F) {
	option_list = list(
		make_option(c('-d', '--efdir'), action = 'store', default = NA, type = 'character'),
		make_option(c('-f', '--filename'), action = 'store', default = NA, type = 'character'),
		make_option(c('-j', '--jobname'), action = 'store', default = NA, type = 'character')
	)
	opt = parse_args(OptionParser(option_list = option_list))

	if (!is.na(opt$filename) & !is.na(opt$jobname) & !is.na(opt$efdir)) {
		print(paste0('Running with file ', opt$filename, ' and jobname ', opt$jobname))
	} else {
		stop('Missing variables')
	}
} else {
	# Default args for interactive mode
	opt = list(
		efdir = '/home/charles/projects/econforecasting',
		filename = '/home/charles/projects/econforecasting/modules/external-import/external-import-cbo.r',
		jobname = 'external-import-cbo'
	)
}

# Load Constants ---------------------------------------------------------------
load_dot_env(file.path(opt$efdir, '.env'))
jobname = opt$jobname
filename = opt$filename
log_path = file.path(opt$efdir, 'logs', paste0(opt$jobname, '.log'))

cat(paste0('jobname=', jobname, '\nfilename=', filename, '\nlog_path=', log_path))

# Run Script ---------------------------------------------------------------
start_time = Sys.time()

script_output = tryCatch(
	{
		system(paste0('echo "" > ', log_path,''))
		sink_path = file.path(log_path)
		sink_conn = file(sink_path, open = 'at')
		sink(sink_conn, append = T, type = 'output')
		sink(sink_conn, append = T, type = 'message')

		message(paste0('----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))

		local({
			source(
				file.path(filename),
				local = T,
				echo = T,
				max.deparse.length = Inf,
				width.cutoff = Inf
				)
		})

		message(paste0('\n----------- FINISHED ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------')))

		sink(type = 'output')
		sink(type = 'message')
		# Store any global variables named validation_log or data_dump from the job
		list(
			success = T,
			validation_log = if (exists('validation_log')) validation_log else NULL,
			data_dump = if (exists('data_dump')) data_dump else NULL,
			err_message = NULL
			)
	},
	error = function(e) list(
		success = F,
		validation_log = if (exists('validation_log')) validation_log else NULL,
		data_dump = if (exists('data_dump')) data_dump else NULL,
		err_message = conditionMessage(e)
		)
	)

end_time = Sys.time()

# Read Log Output ---------------------------------------------------------------
stdout = readChar(log_path, file.info(log_path)$size)
sink(type = 'output')
sink(type = 'message')

# Collect All Output ---------------------------------------------------------------
log_results = list(
	jobname = jobname,
	is_interactive = interactive(),
	is_success = script_output$success,
	start_dttm = start_time,
	end_dttm = end_time,
	run_secs = round(as.numeric(end_time - start_time, 'secs')),
	validation_log = {
		if (is.list(script_output$validation_log)) toJSON(script_output$validation_log, auto_unbox = T)
		else script_output$validation_log
		},
	data_dump = {
		if (is.list(script_output$data_dump)) toJSON(script_output$validation_log, auto_unbox = T)
		else script_output$data_dump
	},
	err_message = script_output$err_message,
	stdout = stdout
)

# Push into DB ---------------------------------------------------------------
pg_conn = dbConnect(
	RPostgres::Postgres(),
	dbname = Sys.getenv('DB_DATABASE'),
	host = Sys.getenv('DB_SERVER'),
	port = Sys.getenv('DB_PORT'),
	user = Sys.getenv('DB_USERNAME'),
	password = Sys.getenv('DB_PASSWORD')
)

log_results_clean = unlist(lapply(log_results, \(x) {
	if (is.null(x) || length(x) == 0) NA
	else if (is.POSIXct(x)) format(x, '%Y-%m-%d %H:%M:%S %Z')
	else x
}))

colnames_str = paste0(names(log_results_clean), collapse = ',')
bind_str = paste0(paste0('$', seq_along(log_results_clean)), collapse = ',')

rs = dbSendQuery(
	pg_conn,
	paste0('INSERT INTO jobscript_runs (', colnames_str, ') VALUES (', bind_str, ')'),
	unname(log_results_clean)
	)
dbClearResult(rs)

dbDisconnect(pg_conn)
