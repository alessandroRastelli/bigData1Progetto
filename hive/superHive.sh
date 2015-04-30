#! /usr/bin/env bash



# configurare i path di questo script (vedi CONFIG)

# lanciare
# ./superHive.sh 2>&1 | tee myhive.log.txt


# splittare i files
# awk -f split.awk myhive.log.txt




/* CONFIG */
BINPUT="/mnt/"
BOUTPUT="/mnt/output/"
/* END CONFIG */


DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
cd $DIR

for SIZE in 1M 10M 100M 50M
do
	
	INPUT=$BINPUT/esempio-$SIZE.txt
	OUTPUT=$BOUTPUT/hive-$SIZE
	LOG=$BOUTPUT/hive-$SIZE.log.txt
	
	echo "STARTING ES1..."
	echo $SIZE
	hive -f es1.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT
	echo "-|"
	
	echo "STARTING ES2..."
	echo $SIZE
	hive -f es2.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT
	echo "-|"
	
	echo "STARTING ES3..."
	echo $SIZE
	hive -f es3.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT
	echo "-|"
	
	echo "STARTING OPT1..."
	echo $SIZE
	hive -f opt1.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT
	echo "-|"
	
	echo "STARTING OPT2..."
	echo $SIZE
	hive -f opt2.hql -hiveconf INPUT=$INPUT -hiveconf OUTPUT=$OUTPUT
	echo "-|"
	
done




