#!/usr/bin/env python
import json
import random
import sys
import threading
import boto3
import argparse

sort_key = ''
target_region = ''
target_table = ''
target_profile = ''
partition_key = ''
client = ''
ids = []
table = ''


def main(argv):
    parser = argparse.ArgumentParser(description='Clear DynamoDB tables')
    parser.add_argument('--table', '-t', required=True, help='DynamoDB Table')
    parser.add_argument('--key', '-k', required=True, help='Partition Key')
    parser.add_argument('--region', '-r', required=True, help='AWS Region')
    parser.add_argument('--profile', '-p', required=False, help='Optional AWS Profile')
    parser.add_argument('--sort', '-s', required=False, help='Optional Sort Key')
    parser.add_argument('--threadsdel', '-td', required=False, help='Threads for deletion')
    parser.add_argument('--threadsscan', '-ts', required=False, help='Threads for scan')

    args = parser.parse_args()

    global client, target_profile, target_region, target_table, sort_key, partition_key, ids, table, total_threads_scan, total_threads_del
    if args.profile:
        target_profile = args.profile
    target_region = args.region
    target_table = args.table
    if args.sort:
        sort_key = args.sort
    partition_key = args.key

    if args.threadsscan:
        total_threads_scan = int(args.threadsscan)
    else:
        total_threads_scan = 1

    if args.threadsdel:
        total_threads_del = int(args.threadsdel)
    else:
        total_threads_del = 1

    print('Table: ' + target_table)
    print('Profile: ' + target_profile)
    print('Region: ' + target_region)
    print('Sort Key:' + sort_key)
    print('Partition Key:' + partition_key)
    print('Threads for scan:' + str(total_threads_scan))
    print('Threads for delete:' + str(total_threads_del))

    if target_profile:
        session = boto3.Session(profile_name=target_profile, region_name=target_region)
    else:
        session = boto3.Session(region_name=target_region)
    client = session.client('dynamodb')

    table = session.resource('dynamodb').Table(target_table)

    run_scan_in_parallel()

    run_delete_in_parallel()


def scan_foo_table(segment, total_segments):
    print('Looking at segment ' + str(segment))
    thread_count = 0

    if sort_key != '':
        response = client.scan(
            TableName=target_table,
            Segment=segment,
            TotalSegments=total_segments,
            AttributesToGet=[sort_key, partition_key]
        )
        for each in response['Items']:
            ids.append({partition_key: each[partition_key], sort_key: each[sort_key]})
    else:
        response = client.scan(
            TableName=target_table,
            Segment=segment,
            TotalSegments=total_segments,
            AttributesToGet=[partition_key]
        )
        for each in response['Items']:
            ids.append(each[partition_key])

    thread_count = thread_count + len(response['Items'])

    while True:
        if 'LastEvaluatedKey' in response:
            if sort_key != '':
                response = client.scan(
                    TableName=target_table,
                    Segment=segment,
                    TotalSegments=total_segments,
                    AttributesToGet=[sort_key, partition_key],
                    ExclusiveStartKey=response['LastEvaluatedKey']

                )
                for each in response['Items']:
                    ids.append({partition_key: each[partition_key], sort_key: each[sort_key]})
            else:
                response = client.scan(
                    TableName=target_table,
                    Segment=segment,
                    TotalSegments=total_segments,
                    AttributesToGet=[partition_key],
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                for each in response['Items']:
                    ids.append(each[partition_key])

            thread_count = thread_count + len(response['Items'])
        else:
            break

    print('Segment ' + str(segment) + ' returned ' + str(thread_count))


def delete_items(thread_id, ids_total_count):
    print('Running at thread_Id ' + str(thread_id))

    last_item = int(ids_total_count / total_threads_del * (thread_id + 1))

    # First thread starts with 1st item till the end of the first range
    if thread_id == 0:
        print("Thread 0 will get range 0 to " + str(last_item))
        thread_ids = ids[:last_item]
    else:
        # ANy other thread starts with the end of the previous range and goes till the end of own range range
        previous_last_item = int(ids_total_count / total_threads_del * thread_id)
        print("Thread " + str(thread_id) + " will get range " + str(previous_last_item + 1) + " to " + str(last_item))
        thread_ids = ids[previous_last_item + 1:last_item]

    print('Deleting count for thread_Id ' + str(thread_id) + " === " + str(len(thread_ids)))

    count = 0
    # batch_writer flushes batches of 25 deletion requests once stack hits 25 items automatically
    with table.batch_writer() as batch:
        if sort_key != '':
            # Split by delimiter <!>
            for id in thread_ids:
                print("Id >>>>>>" + json.dumps({
                    partition_key: id[partition_key],
                    sort_key: id[sort_key]
                }, indent=4))

                batch.delete_item(
                    Key={
                        partition_key: id[partition_key]["S"],
                        sort_key: id[sort_key]["S"]
                    }
                )
                count = count + 1
                if count == 1000:
                    print("Deleted 1k and going. ThreadId = ")
                count = 0
        else:
            for id in thread_ids:
                batch.delete_item(
                    Key={
                        partition_key: id.values()[0]
                    }
                )
                count = count + 1
                if count == 1000:
                    print("Deleted 1k and going. ThreadId = ")
                count = 0


def run_scan_in_parallel():
    thread_list = []

    for i in range(total_threads_scan):
        # Instantiate and store the thread
        thread = threading.Thread(target=scan_foo_table, args=(i, total_threads_scan))
        thread_list.append(thread)
        # Start threads
    for thread in thread_list:
        thread.start()
        # Block main thread until all threads are finished
    for thread in thread_list:
        thread.join()


def run_delete_in_parallel():
    thread_list_del = []
    # Shuffle keys to spread ids from different partitions evenly and decrease chance of partition throttling
    random.shuffle(ids)
    idsTotalCount = len(ids)
    print('Deleting count :' + str(idsTotalCount))

    #   No need for multiple threads if table is almost empty
    if idsTotalCount < 1000:
        total_threads_del = 1

    for i in range(total_threads_del):
        # Instantiate and store the thread
        thread_del = threading.Thread(target=delete_items, args=(i, idsTotalCount))
        thread_list_del.append(thread_del)
        # Start threads
    for thread_del in thread_list_del:
        thread_del.start()
        # Block main thread until all threads are finished
    for thread_del in thread_list_del:
        thread_del.join()


if __name__ == "__main__":
    main(sys.argv)
