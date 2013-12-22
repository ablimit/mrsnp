#! /bin/bash

cd

hadoop fs -get s3://aaji/scratch/s3cfg .s3cfg

# to copy jar 
s3cmd get s3://aaji/scratch/snpcode/snp.jar snp.jar


