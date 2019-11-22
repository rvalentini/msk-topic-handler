import unittest
from core.lambda_function import handler

import httpretty
import requests
import json
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)


CREATE = {
    "RequestType": "Create",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName" : "test-topic-1",
        "Partitions" : "20",
        "Replications": "1" ,
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "RetentionTimeInMs":"5000"
    }
}

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

UPDATE = {
    "RequestType": "Update",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName" : "test-topic-3",
        "Partitions" : "15",
        "Replications": "1" ,
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "CleanupPolicy":"compact"
    },
    "OldResourceProperties" : {
        "Replications" : "1",
        "TopicName" : "test-topic-1",
        "RetentionTimeInMs":"5000"
   }
}



class MockContext(object):
    function_name = "test-function"
    ms_remaining = 9000

    @staticmethod
    def get_remaining_time_in_millis():
        return MockContext.ms_remaining


class TestCloudformationSignals(unittest.TestCase):
    def setUp(self):
        logger.info('Setting up mock server ...')
        httpretty.enable()  
        httpretty.register_uri(httpretty.PUT, "http://cf-callback.com/", status=200)


    def test_create_signal(self):
        """
        Test that topics are successfully created on 'create' signal
        """
        
        handler(CREATE, MockContext)
    
        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        self.assertEqual(status, "SUCCESS")

    def test_update_signal(self):
        """
        Test that topics are successfully created AND/OR deleted on 'update' signal
        """

        handler(UPDATE, MockContext)
        
        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        self.assertEqual(status, "SUCCESS")


        self.assertEqual(1, 1)

    def test_delete_signal(self):
        """
        Test that topics are successfully deleted on 'delete' signal
        """

        handler(DELETE, MockContext)
        
        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        self.assertEqual(status, "SUCCESS")


if __name__ == "__main__":
    unittest.main()