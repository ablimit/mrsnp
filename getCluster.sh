#! /bin/bash

# --ami-version 3.0.2  --ami-version 1.0 
# --num-instances=18 --instance-type=c1.xlarge --master-instance-type=m1.medium 

elastic-mapreduce --create --alive --enable-debugging  --instance-group master --instance-type m1.medium --instance-count 1 --instance-group core --instance-type m1.medium --instance-count 20 --instance-group task --instance-type m1.medium --instance-count 50 --name 'snp cluster' --bootstrap-action s3://elasticmapreduce/bootstrap-actions/configure-hadoop --args "-m,mapred.tasktracker.reduce.tasks.maximum=2" --region us-east-1 --log-uri 's3://aaji/scratch/logs' --with-termination-protection --key-pair aaji  --jar s3://aaji/scratch/snpcode/snp.jar --main-class Vcftoplink --arg s3://aaji/data/snp --arg s3n://aaji/scratch/snpout  --arg 1 --arg 93 --arg 0.001  --arg 93834

# --instance-group master --instance-type m1.medium --instance-count 1 \
# --instance-group core   --instance-type m1.medium --instance-count 5  --bid-price 0.028
# --instance-group task   --instance-type c1.xlarge --instance-count 14 --bid-price 0.028

