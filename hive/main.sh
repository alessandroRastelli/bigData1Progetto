#! /usr/bin/env bash

/* CONFIG */
INPUT="../data/generator/sample/esempio.txt"
OUTPUT="../data/output/hive/"
EXTLIB="../target/bigdata-1.0.1.jar"
/* END CONFIG */


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

echo "STARTING ES1..."
hive -f es1.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT

echo "STARTING ES2..."
hive -f es2.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT

echo "STARTING ES3..."
hive -f es3.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT

echo "STARTING OPT1..."
hive -f opt1.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT

hive -f opt2.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT