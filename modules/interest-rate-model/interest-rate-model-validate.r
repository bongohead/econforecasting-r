# Initialize ----------------------------------------------------------

## Set Constants ----------------------------------------------------------
JOB_NAME = 'interest-rate-model-validate'
EF_DIR = Sys.getenv('EF_DIR')

## Log Job ----------------------------------------------------------
if (interactive() == FALSE) {
	sink_path = file.path(EF_DIR, 'logs', paste0(JOB_NAME, '.log'))
	sink_conn = file(sink_path, open = 'at')
	system(paste0('echo "$(tail -50 ', sink_path, ')" > ', sink_path,''))
	lapply(c('output', 'message'), function(x) sink(sink_conn, append = T, type = x))
	message(paste0('\n\n----------- START ', format(Sys.time(), '%m/%d/%Y %I:%M %p ----------\n')))
}

## Load Connection Info ----------------------------------------------------------
db = connect_db(secrets_path = file.path(EF_DIR, 'model-inputs', 'constants.yaml'))
# run_id = log_start_in_db(db, JOB_NAME, 'interest-rate-model')
validate = list()
expects = list()
raw_df = collect(tbl(db, sql("SELECT * FROM forecast_values WHERE forecast = 'int'")))

# Validate FFR ----------------------------------------------------------
local({

	raw_df %>%
		filter(., varname == 'ffr') %>%
		filter(., vdate == max(vdate))


})

# Validate Treasury ----------------------------------------------------------
local({

	treasury_df =
		raw_df %>%
		filter(., str_detect(varname, '^t\\d\\dy$')) %>%
		filter(., vdate == max(vdate))

	treasury_df %>%
		group_by(., varname) %>%
		summarize(., max_vdate = max(vdate), min_vdate = min(vdate), n_obs = n())


})
