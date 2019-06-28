# aws_dynamodb_table_cleanup
AWS DynamoDB Table Cleanup script written on Python

1. Scans table using Parallel Scans (https://aws.amazon.com/blogs/aws/amazon-dynamodb-parallel-scans-and-other-good-news/). Works for table with Partition key and with Partition and Sort keys
2. Shuffles scanned keys to spread deletion requests load over all partition evenly and avoid partitions throttling
3. Deletes records in batches of 25 items in parallel threads 

Input arguments:
--table, -t -        Required DynamoDB Table
--key, -k -          Required Partition Key
--region, -r -       Required AWS Region
--profile, -p -      Optional AWS Profile
--sort, -s -         Optional Sort Key
--threadsdel, -td -  Optional threads for deletion number
--threadsscan, -ts - Optional threads for scan number
