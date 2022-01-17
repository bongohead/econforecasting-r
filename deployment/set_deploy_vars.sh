# --- On Windows, run this in Git Bash ---
# Deployment
HOSTNAME=`hostname`
EF_R=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "/usr/bin/Rscript" || echo "C:/Program Files/R/R-4.1.2/bin/Rscript.exe")
EF_DIR=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "/home/charles/projects/econforecasting" || echo "D:/Onedrive/__Projects/econforecasting")
EF_PY=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "/home/charles/miniconda3" || echo "C:/Users/Charles/miniconda3")

echo ${EF_DIR}
echo ${EF_PY}

# Push environmental variables to R home directory/.Renviron
# HOME_DIR=$("${EF_R}" -e 'cat(Sys.getenv("HOME"))')
HOME_DIR=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "/home/charles" || echo "C:/Users/Charles/Documents")

rm -rf ${HOME_DIR}/.Renviron

cat <<EOF >${HOME_DIR}/.Renviron
EF_DIR = ${EF_DIR}
EF_PY = ${EF_PY}
RETICULATE_PYTHON = ${EF_PY}
EOF

# Conda script is located in miniconda/condabin
CONDA=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "$EF_PY/condabin/conda" || echo "$EF_PY/condabin/conda.bat")
# Create new env
$CONDA update -n base -c defaults conda
$CONDA create -n econforecasting
$CONDA list -n econforecasting
$CONDA config --add channels conda-forge
$CONDA config --set channel_priority strict 
# Install dirs
$CONDA install -n econforecasting -c conda-forge pandas
$CONDA install -n econforecasting -c conda-forge camelot-py