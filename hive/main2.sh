#! /usr/bin/env bash

INPUT="../data/generator/sample/esempio-10M.txt"
OUTPUT="../data/output/hive/"
EXTLIB="../target/bigdata-1.0.1.jar"



DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

echo "MacBook:~ alessandrorastelli$ hive -f opt2.hql -hiveconf INPUT=/Users/alessandrorastelli/input/esempio-10M.txt -hiveconf OUTPUT=/Users/alessandrorastelli/hive/output-50M"
hive -f opt2.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT