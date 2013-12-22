#! /bin/bash

cd

hadoop fs -get s3://aaji/scratch/s3cfg .s3cfg

# to copy jar 
s3cmd get s3://aaji/scratch/snpcode/snp.jar snp.jar

# hadoop jar snp.jar Vcftoplink s3://aaji/data/snp s3://aaji/scratch/snpout 1 93 0.001 93834

