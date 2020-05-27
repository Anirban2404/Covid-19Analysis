#!/bin/bash
CURRDIR=${PWD}
echo "Current location: ${CURRDIR}"

BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"

# cd into a directory with space --- use "{DIR}"
cd "${BASEDIR}"
echo "Script executed from: ${PWD}"

time ./src/py_src/DataPreperation/DataPrepare.sh
echo ">>> CSV Creation Completed."

mysql -u covidAnalyst -p -e 'SET GLOBAL local_infile=1;'
time ./src/py_src/DataLoad/DataLoad.sh
echo ">>> Data Load Completed."

cd "${CURRDIR}"
echo "Current location: ${PWD}"
echo ">>> Data Preperation and Load completed."
