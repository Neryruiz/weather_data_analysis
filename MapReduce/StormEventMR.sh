#!/bin/bash
#this file is to run command to run java mapreduce file in hadoop framework
echo "deleteing hadoop /output"
hadoop fs -rm -r /output

echo "Compling java code"
javac -cp `hadoop classpath` StormEventMR.java

echo "creating jar file: SEMR.jar"
jar cvf SEMR.jar *.class

echo "Running jar file in hapoop"
hadoop jar SEMR.jar StormEventMR /stormevent /output

echo "output file created in hadoop"
