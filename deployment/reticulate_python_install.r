library(reticulate)

install_python(version = "3.8.7")
use_python_version('3.8.7')
virtualenv_create('econforecasting')
virtualenv_install('econforecasting', 'camelot-py')
virtualenv_install('econforecasting', 'opencv-python')