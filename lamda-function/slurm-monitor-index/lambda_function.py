import os
import re
import traceback
from datetime import datetime
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk


###############################################################################
# entry point from sqs trigger
###############################################################################
def lambda_handler(event, context):

    # initialize context
    sm_context = {}

    openSearch = {}
    openSearch["host"] = os.environ['OPENSEARCH_HOST']
    openSearch["port"] = 443
    openSearch["auth"] = (os.environ['OPENSEARCH_USERNAME'], os.environ['OPENSEARCH_PASSWORD'])
    sm_context["openSearch"] = openSearch

    stat = {}
    stat["message_received"] = len(event['Records'])
    stat["message_processed"] = 0
    stat["job_received"] = 0
    stat["job_indexed"] = 0
    sm_context["stat"] = stat

    openSearch["client"] = OpenSearch(
        hosts=[{'host': openSearch["host"], 'port': openSearch["port"]}],
        http_compress=True,
        http_auth=openSearch["auth"],
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=False,
        ssl_show_warn=False)

    # process sqs messages 
    for message in event['Records']:
        process_message(sm_context, message)

    openSearch["client"].close()
    print("{} Done: {}".format(datetime.now(), stat))


###############################################################################
# process one sqs message
###############################################################################
def process_message(sm_context, message):
    try:
        misc = {}
        sm_context["misc"] = misc

        # fetch message attributes
        if len(message["attributes"]) > 0:
            sent_ts = message["attributes"].get('SentTimestamp')
            if sent_ts:
                sqs_sent_ts = datetime.fromtimestamp(int(sent_ts)/1000)
                misc["sqs_sent_ts"] = sqs_sent_ts
                index_suffix = sqs_sent_ts.strftime('%Y%m')
                misc["index_suffix"] = index_suffix

        if len(message["messageAttributes"]) > 0:
            capture_src_obj = message["messageAttributes"].get('capture_src')
            if capture_src_obj:
                capture_src = capture_src_obj.get('stringValue')
                misc["capture_src"] = capture_src

            capture_ts_obj = message["messageAttributes"].get('capture_ts')
            if capture_ts_obj:
                capture_ts = capture_ts_obj.get('stringValue')
                misc["capture_ts"] = capture_ts

        if sqs_sent_ts and capture_src and capture_ts:
            match capture_src:
                case "scontrol_show_job":
                    jobs = parse_scontrol_show_job(message["body"], misc)
                    do_indexing(sm_context, jobs)
                    sm_context["stat"]["message_processed"] += 1

                case "squeue":
                    jobs = parse_squeue(message["body"], misc)
                    do_indexing(sm_context, jobs)
                    sm_context["stat"]["message_processed"] += 1

                case _:
                    print("{} Unable to process capture_src: {}".format(datetime.now(), capture_src))

    except Exception as err:
        print("{} Failed to process message: {}\n{}".format(datetime.now(), err, message))
        traceback.print_exc() 


###############################################################################
# convert message body to dictionary
###############################################################################
def parse_scontrol_show_job(body, misc):
    jobs = []
    job = {}

    stopList = ["Unknown", "None", "(null)", ""]
    pattern = re.compile(r"(.+?=(?:domain users.*?|.*?))\s+")
    for match in pattern.finditer(body):
        item = re.split("=", match.group(1), 1)
        if (item[0] == 'JobId') and (len(job) > 0):
            job['capture_ts'] = misc["capture_ts"]
            job['capture_src'] = misc["capture_src"]
            job['sqs_sent_ts'] = misc["sqs_sent_ts"]
            jobs.append(job)
            job = {}

        match len(item):
            case 2:
                if item[1] not in stopList:
                    job[item[0]] = item[1]

            case 1:
                pass

            case _:
                print("{} Invalid item: {}".format(datetime.now(), item))

    if len(job) > 0:
        job['capture_ts'] = misc["capture_ts"]
        job['capture_src'] = misc["capture_src"]
        job['sqs_sent_ts'] = misc["sqs_sent_ts"]
        jobs.append(job)

    return jobs


###############################################################################
# convert message body to dictionary
###############################################################################
def parse_squeue(body, misc):
    jobs = []
    job = {}

    stopList = ["Unknown", "None", "(null)", ""]
    pattern = re.compile(r"(.+?)(?:\n+|$)")
    for i, match in enumerate(pattern.finditer(body)):
        if i == 0:
            header = re.split(r"\|", match.group(1))
        else:
            items = re.split(r"\|", match.group(1))
            for j, item in enumerate(items):
                if item not in stopList:
                    job[header[j]] = item

            job['capture_ts'] = misc["capture_ts"]
            job['capture_src'] = misc["capture_src"]
            job['sqs_sent_ts'] = misc["sqs_sent_ts"]
            jobs.append(job)
            job = {}

    return jobs


###############################################################################
# openSearch bulk indexing
###############################################################################
def do_indexing(sm_context, jobs):
    sm_context["stat"]["job_received"] += len(jobs)
    client = sm_context["openSearch"]["client"]
    misc = sm_context["misc"]

    indexName = 'sm-{}-{}'.format(misc["capture_src"], misc["index_suffix"])
    if not client.indices.exists(indexName):
        do_create_index(sm_context, indexName)

    actions = []
    for document in jobs:
        actions.append({
            "_op_type": "index", 
            "_index": indexName,
            "_source": document
        })

    success, failed = bulk(client, actions)
    sm_context["stat"]["job_indexed"] += success
    if len(failed) > 0:
        print('{} bulk: success={} | failed={}'.format(datetime.now(), success, failed))


###############################################################################
# create openSearch index
###############################################################################
def do_create_index(sm_context, indexName):
    sm_context["openSearch"]["client"].indices.create(index=indexName)
    print('{} index [{}] created'.format(datetime.now(), indexName))
