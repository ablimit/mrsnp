#! /bin/bash

rm *.class

hadooplibpath=/home/hadoop/.versions/1.0.3/share/hadoop
clilibpath=/home/hadoop/lib

javac -cp .:${hadooplibpath}/hadoop-core-1.0.3.jar:${clilibpath}/commons-cli-1.2.jar Vcftoplink.java

jar -cvf snp.jar *.class

