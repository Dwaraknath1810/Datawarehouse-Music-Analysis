# Datawarehouse-Music-Analysis
# Architecture
![image](https://user-images.githubusercontent.com/88726925/232324212-41fed1e5-1577-4221-b706-e744454f3e9c.png)
The workflow illustrated in the diagram consists of these high-level steps:
  1)The user uploads a CSV file into the source folder in Amazon S3.
  2)An Amazon S3 notification event initiates an AWS Lambda function that starts the Step Functions state machine.
  3)The Lambda function validates the schema and data type of the raw CSV file.
  4)Depending on the validation results:
      a)If validation of the source file succeeds, the file moves to the stage folder for further processing.
      b)If validation fails, the file moves to the error folder, and an error notification is sent through Amazon Simple Notification Service (Amazon SNS).
  5)An AWS Glue crawler creates the schema of the raw file from the stage folder in Amazon S3.
  6)An AWS Glue job transforms, compresses, and partitions the raw file into Parquet format.
  7)The AWS Glue job also moves the file to the transform folder in Amazon S3.
  8)The AWS Glue crawler creates the schema from the transformed file. The resulting schema can be used by any analytics job. You can also use Amazon Athena to run ad-hoc queries.
  9)If the pipeline completes without errors, the schema file is moved to the archive folder. If any errors are encountered, the file is moved to the error folder instead.
  10)Amazon SNS sends a notification that indicates success or failure based on the pipeline completion status.
