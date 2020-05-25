#!/bin/bash
CURRDIR=${PWD}
echo "Current location: ${CURRDIR}"

BASEDIR=$(dirname $0)
echo "Script location: ${BASEDIR}"

# cd into a directory with space --- use "{DIR}"
cd "${BASEDIR}"
echo "Script executed from: ${PWD}"

time ./DBLoad_Global.py
time ./DBLoad_USA.py
time ./DDBLoad_USA_State.py

cd "${CURRDIR}"
echo "Current location: ${PWD}"