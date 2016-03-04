#!/usr/bin/env bash
mkdir ./data
## declare an array variable
declare -a arr=("machine_events" "job_events" "task_events" "task_usage")

## now loop through the above array
url="gs://clusterdata-2011-2/"
for folder in "${arr[@]}"
do
   for i in 0 1 2 3 4
   do
        gsutil cp "$url/$folder/part-0000$i-of-00500.csv.gz"
        gsutil cp "$url/$folder/part-0000$i-of-00001.csv.gz"
   done
   cd folder
   gzip -d *
   cd ../
   # or do whatever with individual element of the array
done