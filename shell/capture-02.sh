#!/bin/env bash

# #######################################################################
# capture slurm job information via the "squeue --all --format='%all'" command
# send information to SQS
# This script is designed to run from crontab once every 5 minutes
# #######################################################################
queue_url="https://sqs.us-east-1.amazonaws.com/874526227640/slurm-monitor-capture-job-01"
timestamp_format="%Y-%m-%dT%T"
timestamp=$(date +$timestamp_format)
msg_attributes="capture_src={StringValue=squeue,DataType=String},capture_ts={StringValue=${timestamp},DataType=String}"

echo "$timestamp INFO start"

# fetch squeue output
body=""
#readarray -t squeue_output < <(cat ./test/squeue.output)
readarray -t squeue_output < <(/opt/slurm/bin/squeue --all --format="%all")

# send in batches of 10
for i in "${!squeue_output[@]}"; do
    if [ $i -eq 0 ]; then
        header="${squeue_output[i]}"
    elif [ $(((i - 1) % 10)) -eq 9 ]; then
        body+=$'\n'"${squeue_output[i]}"
#	echo "$header$body"
        response=$(aws sqs send-message --queue-url $queue_url --message-body "$header$body" --message-attributes "$msg_attributes")
	echo $response
	body=""
    else
	body+=$'\n'"${squeue_output[i]}"
    fi
done

# send the final batch
if [ ${#body} -gt 0 ]; then
#    echo "$header$body"
    response=$(aws sqs send-message --queue-url $queue_url --message-body "$header$body" --message-attributes "$msg_attributes")
    echo $response
fi

timestamp=$(date +$timestamp_format)
echo "$timestamp INFO finish"
