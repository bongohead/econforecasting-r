# Analyze historical performance
DIR = Sys.getenv('EF_DIR')
DL_DIR = file.path(Sys.getenv('EF_DIR'), 'tmp')
INPUT_DIR = file.path(Sys.getenv('EF_DIR'), 'model-inputs') # Path to directory with constants.r (SQL DB info, SFTP info, etc.)
OUTPUT_DIR = file.path(Sys.getenv('EF_DIR'), 'model-outputs')
VINTAGE_DATE_START = as.Date('2021-10-20')
VINTAGE_DATE_END = Sys.Date()

# Pull vintage data
local({
	
	
	
	
	
})
