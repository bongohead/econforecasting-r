DIR = 'D:/Onedrive/__Projects/econforecasting'
DL_DIR = 'D:/Onedrive/__Projects/econforecasting/tmp'
PACKAGE_DIR = 'D:/Onedrive/__Projects/econforecasting/r-package'
INPUT_DIR = 'D:/Onedrive/__Projects/econforecasting/model-inputs'
OUTPUT_DIR = 'D:/Onedrive/__Projects/econforecasting/model-outputs'
VINTAGE_DATE = as.Date('2021-09-04')
RESET_ALL = TRUE


library(dplyr)
library(readr)
library(stringr)


# Directory
sinkFile = file(file.path(DIR, 'sink.txt'), open = 'wt')
sink(sinkFile, type = 'output')
sink(sinkFile, type = 'message')

message('Run :', Sys.Date())

system(
	'"C:/Program Files/Git/bin/bash.exe" -c  "tail -n 50 -f "D:/OneDrive/__Projects/econforecasting/sink.txt""',
	intern = F, wait = F,
	invisible = FALSE,
	minimized = FALSE
)

# Convert to R -> Run
runRes = lapply(list('m1-pull-data.rmd', 'm2-nowcast.rmd', 'm3-qual.rmd', 'm4-csm.rmd', 'm5-sql.rmd'), function(filename) {
	
	message('\n\n########## Started - ', filename, ': ', Sys.time(), ' ##########\n\n')
	
	sourcefile = file.path(DIR, filename)
	tempfile = file.path(DIR, 'purl.r')
	# Use purl to extract main R code (minus first chunk) from Rmd and convert it to a function
	if (file.exists(tempfile)) file.remove(tempfile)
	knitr::purl(input = sourcefile, output = tempfile)
	
	run_model =
		readr::read_lines(tempfile) %>%
		paste0(., collapse = '\n') %>%
		str_glue(
		'
		# This file has been auto-generated by model-nowcasts-backtest.rmd for backtesting -
		# to modify original code with persistance, modify original code in model-nowcasts.rmd
		run_model = function(
			DIR = \'{DIR}\',
			DL_DIR = \'{DL_DIR}\',
			PACKAGE_DIR = \'{PACKAGE_DIR}\',
			INPUT_DIR = \'{INPUT_DIR}\',
			OUTPUT_DIR = \'{OUTPUT_DIR}\',
			VINTAGE_DATE = as.Date(\'{VINTAGE_DATE}\'),
			RESET_ALL = {RESET_ALL}
			) {{
			{body}
			return()
			}}',
		body = .
		) %>%
		parse(text = ., keep.source = FALSE) %>%
		eval(.)
	
	# "C:\Program Files\Git\bin\bash.exe" -c  "ls C:/Users"
	# tail -n 50 -f 'D:/OneDrive/__Projects/econforecasting/sink.txt'
	# 
	run_model()

	file.remove(tempfile)
	message('\n\n########## Finished - ', filename, ': ', Sys.time(), ' ##########\n\n')
})

sink()
