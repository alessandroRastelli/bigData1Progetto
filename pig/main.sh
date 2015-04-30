#! /usr/bin/env bash

/* CONFIG */
INPUT="../data/generator/sample/esempio.txt"
OUTPUT="../data/output/pig/"
EXTLIB="../target/bigdata-1.0.1.jar"

ENGINE="local"
#ENGINE="mapred"

/* END CONFIG */




DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

echo "STARTING ES1..."
pig -x $ENGINE -f es1.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT

echo "STARTING ES2..."
pig -x $ENGINE -f es2.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT

echo "STARTING ES3..."
pig -x $ENGINE -f es3.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT

echo "STARTING OPT1..."
pig -x $ENGINE -f opt1.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT

echo "STARTING OPT2..."
pig -x $ENGINE -f opt2.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT -param EXTLIB=$EXTLIB