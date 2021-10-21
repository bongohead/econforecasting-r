DIR = Sys.getenv('EF_DIR')

# Directory
sinkFile = file(file.path(DIR, 'pull-external-forecasts-log.txt'), open = 'wt')
sink(sinkFile, type = 'output')
sink(sinkFile, type = 'message')

message('----------------------------------
Run :', Sys.Date(), '
----------------------------------
')
source(file.path(DIR, 'pull-external-forecasts.r'))

sink()
