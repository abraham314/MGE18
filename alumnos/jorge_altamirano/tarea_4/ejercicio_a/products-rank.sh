#!/bin/bash
#@author 2018 Jorge III Altamirano Astorga
#@description Counts the flights using Apache Pig
export PIG_FILE=/dfs/ejercicio_a/products-rank.pig
export  PIG_OUT=/tarea_4/ejercicio_a
echo -n "Deleting output file... "
hdfs dfs -rm -r -f -skipTrash $PIG_OUT
echo 
echo "Starting pig of $PIG_FILE ..."
pig $PIG_FILE
echo
echo "Showing output file:"
hdfs dfs -cat $PIG_OUT/part-r-00000

echo "Copying file to dir"
hdfs dfs -copyToLocal $PIG_OUT/part-r-00000 /dfs/ejercicio_a/output.txt

echo
echo "End!"
