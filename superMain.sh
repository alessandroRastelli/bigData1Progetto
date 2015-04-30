#! /usr/bin/env bash

/* CONFIG */
BINPUT="/mnt/"
BOUTPUT="/mnt/output/"
EXTLIB="/home/hadoop/script/bigdata-1.0.1.jar"
ENGINE="mapred"
H="/user/hadoop/"
/* END CONFIG */


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR


for SIZE in 1M 10M 100M 50M
do
	echo $SIZE;
	
	hdfs dfs -put $BINPUT/esempio-$SIZE.txt $H
	
	
	#hive
	
	INPUT=$BINPUT/esempio-$SIZE.txt
	OUTPUT=$BOUTPUT/hive-$SIZE
	LOG=$BOUTPUT/hive-$SIZE.log.txt
	SUCCESS=$BOUTPUT/hive-$SIZE.done
	
	echo "STARTING ES1..."
	hive -f hive/es1.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING ES2..."
	hive -f hive/es2.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING ES3..."
	hive -f hive/es3.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING OPT1..."
	hive -f hive/opt1.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	touch $SUCCESS
	
	
	#pig
	
	INPUT=$H/esempio-$SIZE.txt
	OUTPUT=$H/pig-$SIZE
	LOG=$BOUTPUT/pig-$SIZE.log.txt
	SUCCESS=$BOUTPUT/pig-$SIZE.done
	
	
	echo "STARTING ES1..."
	pig -x $ENGINE -f pig/es1.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING ES2..."
	pig -x $ENGINE -f pig/es2.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING ES3..."
	pig -x $ENGINE -f pig/es3.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING OPT1..."
	pig -x $ENGINE -f pig/opt1.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT 2>&1 | tee $LOG
	
	echo "STARTING OPT2..."
	pig -x $ENGINE -f pig/opt2.pig -param INPUT=$INPUT -param OUTPUT=$OUTPUT -param EXTLIB=$EXTLIB 2>&1 | tee $LOG
	
	touch $SUCCESS
	
	
	
done




