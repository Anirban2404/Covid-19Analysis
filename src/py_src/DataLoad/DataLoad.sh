#!/bin/bash
CURRDIR=${PWD}
echo "Current location: ${CURRDIR}"

BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"

# cd into a directory with space --- use "{DIR}"
cd "${BASEDIR}"
echo "Script executed from: ${PWD}"

time ./DBLoad_Global.py
echo ">>> Global csv file loaded."
time ./DBLoad_USA.py
echo ">>> USA csv file loaded."
time ./DBLoad_USA_State.py
echo ">>> USA state csv file loaded."

cd "${CURRDIR}"
echo "Current location: ${PWD}"
