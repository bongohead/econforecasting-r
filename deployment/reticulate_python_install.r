# On Linux, will need to install libsqlite3
library(reticulate)
install_python(version = "3.8.7")
use_python_version('3.8.7')
virtualenv_create('econforecasting')
virtualenv_install('econforecasting', 'camelot-py')
virtualenv_install('econforecasting', 'opencv-python')


# Reset
library(reticulate)
use_python_version('3.8.7')
virtualenv_remove('econforecasting')
# Look for dir in py_config
system('rm -rf /home/charles/.pyenv')