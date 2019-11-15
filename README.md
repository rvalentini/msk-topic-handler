# msk-topic-handler
Defines a custom Cloudformation resource for managing AWS-MSK Kafka topics.

## How does it work? 

This library defines a special AWS Lambda function, which can handle the following three signals from AWS Cloudformation:
1. CREATE -> New resource should be created
2. UPDATE -> An exisiting resource should be update with new configuration
3. DELETE -> An existing resource should be deleted

The **msk-topic-handler** function reacts to these Cloudformation signals by calling the broker nodes of the configured MSK cluster. Depending on the singnal type, new Kafka topics are created or deleted.

## Usage

A MSK-topic resource definition as part of a Cloudformation script looks like the following:
~~~
TestTopicA: 
    Type: "Custom::MskTopic"
    Properties: 
      ServiceToken: !Sub arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:function:${LambdaFunctionName}
      TopicName: "TEST-TOPIC-A"
      Partitions: 24
      Replications: 3
      BootstrapServers: !Ref BootstrapServers
      ClientId: !Ref ClientId
~~~


* `ServiceToken`  references the custom resource Lambda function
* `TopicName` defines the name of the Kafka topic
* `Partitions`  defines the number of partitions of the Kafka topic
* `Replications`  defines the replication factor of the Kafka topic

## Upload to AWS

In order to upload the Lambda function, which defines the Cloudformation custom resource, into your AWS account, simply execute the **push_to_aws.sh** script. Before you run it, just exchange the `[YOUR-PROFILE]` placeholder with your configured AWS account profile. The script will
1. Download all required dependencies (see requirements.txt)
2. Pack the lambda function code together with all dependencies into a **function.zip** file
3. Update the AWS Lambda function called **msk-topic-handler** inside your AWS account with the zip file  (in case you want to change the name, make sure to adjust the references inside the Cloudformation **msk-topics.yml** file to match your Lambda function name)

## Local Testing

Inside the `/test` directory you can find integration tests for all three CF-signals (CREATE, UPDATE and DELETE).
In order to run the tests, you need to spin up a local Kafka-cluster (e.g. see https://github.com/wurstmeister/kafka-docker).
In case you have too much free time, you could also mock the Kafka topic interactions entirely, any contributions are much appreciated :)

The file `test_lambda_function.py` defines some JSON-objects representing the CF-singnal payloads inline. E.g:
~~~
DELETE = {
    "RequestType": "Delete",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName" : "test-topic-2",
        "Partitions" : "15",
        "Replications": "1" ,
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id"
    }
}
~~~

When you have a Kafka cluster running, simply execute one of the following commands from package root directory
~~~
python -m unittest test.test_lambda_function.TestCloudformationSignals.test_create_signal
python -m unittest test.test_lambda_function.TestCloudformationSignals.test_update_signal
python -m unittest test.test_lambda_function.TestCloudformationSignals.test_delete_signal
~~~
