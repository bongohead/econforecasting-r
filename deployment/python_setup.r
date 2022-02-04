#' Install Python, Create Virtual Environment, and Install Modules
#' 
#' Deployment:
#' (1) Install and update R
#' (2) Install package reticulate
#' (3) Run set_deploy_vars_nopy.sh; update EF_DIR if necessary
#' (4) Run python_setup.r
#' (5) If different, update EF_PY in set_delpoy_vars_nopy.sh with the values printed from (4);
#'     Then rerun set_deploy_vars_nopy.sh.
#' (6) Run renv::restore() if necessary

if (!dir.exists(file.path(Sys.getenv('EF_DIR'), '.virtualenvs'))) {
	dir.create(file.path(Sys.getenv('EF_DIR'), '.virtualenvs'), mode = '0755')
}

# On Linux, will need to install libsqlite3
library(reticulate)
install_python(version = "3.8.7") # Get the path and store as EF_PY set_deploy_vars_nopy.sh
use_python_version('3.8.7')
virtualenv_create(file.path(Sys.getenv('EF_DIR'), '.virtualenvs', 'econforecasting'))
# Camelot-py and opencv for PDF parsing
# Img2pdf for SVG parsing to PDF
# Matplotlib for diagnosing issues with camelot-py
lapply(c('camelot-py', 'opencv-python', 'svglib', 'requests', 'matplotlib'), function(x) 
	virtualenv_install(file.path(Sys.getenv('EF_DIR'), '.virtualenvs', 'econforecasting'), x)
)


# Reset
library(reticulate)
use_python_version('3.8.7')
virtualenv_remove(file.path(Sys.getenv('EF_DIR'), '.virtualenvs', 'econforecasting'))
# Look for dir in py_config
system('rm -rf /home/charles/.pyenv')