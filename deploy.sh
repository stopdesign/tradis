#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
SERVICE_NAME=$( basename $SCRIPT_DIR )

echo -e "Deploying $SERVICE_NAME...\n"

virtualenv -p python3 $SCRIPT_DIR/.venv
source $SCRIPT_DIR/.venv/bin/activate
pip install -U -r $SCRIPT_DIR/requirements.txt


systemctl restart $SERVICE_NAME*

echo -e "\nServices restarted:"
systemctl | grep $SERVICE_NAME

