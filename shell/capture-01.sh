#!/bin/env bash

# #######################################################################
# capture slurm job information via the "scontrol show job -a" command
# send information to SQS
# This script is designed to run from crontab once every 5 minutes
# #######################################################################
queue_url="https://sqs.us-east-1.amazonaws.com/874526227640/slurm-monitor-capture-job-01"
timestamp_format="%Y-%m-%dT%T"
timestamp=$(date +$timestamp_format)
msg_attributes="capture_src={StringValue=scontrol_show_job,DataType=String},capture_ts={StringValue=${timestamp},DataType=String}"

echo "$timestamp INFO start"

# fetch job status
status=$(/opt/slurm/bin/scontrol show job -a)
#status=$(cat ./test/scontrol.output)

# skip if no job found
if [[ $status != "No jobs in the system"* ]]; then

    # separate individual jobs into array elements
    delimiter="JobId="
    status_str=$status$delimiter

    # 1st element is empty
    jobs=()
    while [[ $status_str ]];  
    do
	# add 1 job to array
	jobs+=( "${status_str%%"$delimiter"*}" )

        # remove first job from status_str	
        status_str=${status_str#*"$delimiter"} 
    done

    #remove empty 1st element
    jobs=("${jobs[@]:1}")

    # send to SQS in batches of 10
    body=""
    for i in "${!jobs[@]}"
    do
        body+="$delimiter${jobs[i]}"
	if [ $((i % 10)) -eq 9 ]; then
            response=$(aws sqs send-message --queue-url $queue_url --message-body "$body" --message-attributes "$msg_attributes")
            echo $response
	    body=""
        fi
    done

    # send the final batch
    if [ ${#body} -gt 0 ]; then
        response=$(aws sqs send-message --queue-url $queue_url --message-body "$body" --message-attributes "$msg_attributes")
        echo $response
    fi
fi

timestamp=$(date +$timestamp_format)
echo "$timestamp INFO finish"
