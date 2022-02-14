#' Create logging file and will sink all output and messages to the file.
#'
#' @param job_name The name of the job.
#' @param log_dir The directory to place the log files.
#'
#' @export
create_job_log = function(job_name, log_dir) {
	if (interactive() == FALSE) {
		sink_path = file.path(log_dir, 'logs', paste0(job_name, '.log'))
		sink_conn = file(sink_path, open = 'at')
		system(paste0('echo "$(tail -1000 ', sink_path, ')" > ', sink_path,''))
		lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
		message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
	}
	return()
}
