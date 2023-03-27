#' Run this file to set Environmental variables
#' Set hostname with sudo hostname econforecasting if on Linux VPS
#' 
#' --- On Windows, run this in Git Bash ---

HOSTNAME=`hostname`

# Environmental variables to store
EF_DIR=$([ "$HOSTNAME" = "econforecasting" ] && echo "/home/charles/projects/econforecasting" || echo "D:/Onedrive/__Projects/econforecasting")
# EF_PY=$([ "$HOSTNAME" = "econforecasting" ] && echo "/home/charles/.pyenv/versions/3.8.7/bin/python3.8" || echo "C:/Users/Charles/AppData/Local/r-reticulate/r-reticulate/pyenv/pyenv-win/versions/3.8.7/python.exe")

echo ${EF_DIR}
#echo ${EF_PY}

# Push environmental variables to R home directory/.Renviron
HOME_DIR=$([ "$HOSTNAME" = "econforecasting" ] && echo "/home/charles" || echo "C:/Users/Charles/Documents")

rm -rf ${HOME_DIR}/.Renviron

cat <<EOF >${HOME_DIR}/.Renviron
EF_DIR = ${EF_DIR}
EOF

echo "----- Complete -----"