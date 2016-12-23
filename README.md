# aws-lambda-athena

This program can operate Amazon Athena from AWS Lambda.

# Preparation
1. Fill config/credential file to access to Athena.

# Input
1. Currently, as input value, following columns are acceptable.

|No|Column Name|Description|Required|
|-----------:|:------------:|:------------:|:------------:|
|1|region|Region name for AWS|false|
|2|s3Path|Amazon S3 path for querying Athena|true|
|3|sql|SQL for Athena|true|
|4|columnListStr|Getting column list separated by ","|true|

## Input sample

```
{
  "region": "us-east-1",
  "s3Path": "s3://kojiisd-athena-staging-dir/",
  "sql": "SELECT elbname, requestip,  requestport, backendip, backendport, requestprocessingtime, backendprocessingtime, timestamp FROM sampledb.elb_logs order by timestamp desc limit 10",
  "columnListStr": "elbname, requestip,  requestport, backendip, backendport, requestprocessingtime, backendprocessingtime,  timestamp"
}
```

## Output by above sample
If you execute this program with above sample, you can get following logs in Lambda and CloudWatch.
For Athena database, it uses default sample database "sampledb".

```
result:
elbname, requestip, requestport, backendip, backendport, requestprocessingtime, backendprocessingtime, timestamp
lb-demo,246.140.190.136,63777,250.193.168.100,8888,7.2E-5,0.379241,2014-09-30T01:28:17.587188Z
lb-demo,246.129.63.235,10182,251.36.119.81,8888,4.7E-5,0.013368,2014-09-30T01:18:08.310452Z
lb-demo,248.214.120.18,25187,249.205.250.232,8000,5.9E-5,0.014451,2014-09-30T01:18:08.294613Z
lb-demo,249.125.138.56,64948,241.35.85.250,8888,6.6E-5,0.023272,2014-09-30T01:18:08.266146Z
lb-demo,247.161.61.55,2700,254.243.154.128,8888,7.5E-5,0.026167,2014-09-30T01:18:08.264297Z
lb-demo,241.35.85.250,10182,248.214.120.18,443,4.6E-5,0.013153,2014-09-30T01:18:08.225343Z
lb-demo,250.165.161.74,25187,249.45.101.192,8888,4.8E-5,0.013252,2014-09-30T01:18:08.207854Z
lb-demo,247.161.61.55,2700,248.14.80.185,8888,4.8E-5,0.015274,2014-09-30T01:18:08.175776Z
lb-demo,249.45.101.192,64948,247.161.61.55,8888,1.15E-4,0.020176,2014-09-30T01:18:08.162703Z
lb-demo,253.51.141.83,10182,243.16.92.84,8888,6.6E-5,0.015285,2014-09-30T01:18:08.130488Z
```

# Restrictions and Limitations
1. This program gets colums what you want to get but every values will be gotten as String type.
2. This program is just tested with normal usage. For each error/exception patterns, developer needs to implement.
