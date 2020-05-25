#!/bin/bash
CURRDIR=${PWD}
echo "Current location: ${CURRDIR}"

BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"

# cd into a directory with space --- use "{DIR}"
cd "${BASEDIR}"
echo "Script executed from: ${PWD}"

time ./src/py_src/DataPreperation/DataPrepare.sh
time ./src/py_src/DataPreperation/DataLoad.sh

cd "${CURRDIR}"
echo "Current location: ${PWD}"
