# --- On Windows, run this in Git Bash ---
# Deployment
HOSTNAME=`hostname`
EF_DIR=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "/home/charles/projects/econforecasting" || echo "D:/Onedrive/__Projects/econforecasting")

echo ${EF_DIR}

# Push environmental variables to R home directory/.Renviron
HOME_DIR=$([ "$HOSTNAME" = "econforecasting.com" ] && echo "/home/charles" || echo "C:/Users/Charles/Documents")

rm -rf ${HOME_DIR}/.Renviron

cat <<EOF >${HOME_DIR}/.Renviron
EF_DIR = ${EF_DIR}
EOF