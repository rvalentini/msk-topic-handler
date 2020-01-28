import unittest
from core.lambda_function import handler

import httpretty
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
        "TopicName": "test-topic-1",
        "Partitions": "20",
        "Replications": "1",
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "RetentionTimeInMs": "5000",
        "CleanupPolicy": "compact"
    }
}

DELETE = {
    "RequestType": "Delete",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName": "test-topic-1",
        "Partitions": "15",
        "Replications": "1",
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id"
    }
}

UPDATE_1 = {
    "RequestType": "Update",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName": "test-topic-1",
        "Partitions": "15",  # CHANGED
        "Replications": "1",
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "RetentionTimeInMs": "5000",
        "CleanupPolicy": "compact"
    },
    "OldResourceProperties": {
        "Replications": "1",
        "TopicName": "test-topic-1",
        "CleanupPolicy": "compact",
        "RetentionTimeInMs": "5000",
        "MaxMessageBytes" : "1000012"
    }
}

UPDATE_2 = {
    "RequestType": "Update",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {  # CHANGED -> removal of cleanupPolicy
        "TopicName": "test-topic-1",
        "Partitions": "15",
        "Replications": "1",
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "RetentionTimeInMs": "5000",
    },
    "OldResourceProperties": {
        "Replications": "1",
        "TopicName": "test-topic-1",
        "CleanupPolicy": "compact",
        "RetentionTimeInMs": "5000",
        "MaxMessageBytes" : "1000012"
    }
}

UPDATE_3 = {
    "RequestType": "Update",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName": "test-topic-1",
        "Partitions": "15",
        "Replications": "1",
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "RetentionTimeInMs": "10000",  # CHANGED
        "CleanupPolicy": "compact"
    },
    "OldResourceProperties": {
        "Replications": "1",
        "TopicName": "test-topic-1",
        "RetentionTimeInMs": "5000",
        "MaxMessageBytes" : "1000012"
    }
}

UPDATE_4 = {
    "RequestType": "Update",
    "RequestId": "test-event-id",
    "StackId": "arn/test-stack-id/guid",
    "LogicalResourceId": "TestResourceId",
    "ResponseURL": "http://cf-callback.com",
    "ResourceProperties": {
        "TopicName": "test-topic-1",
        "Partitions": "15",
        "Replications": "1",
        "BootstrapServers": "localhost:9092",
        "ClientId": "test-id",
        "RetentionTimeInMs": "10000",
        "CleanupPolicy": "compact",
        "MaxMessageBytes" : "20000012" #CHANGED
    },
    "OldResourceProperties": {
        "Replications": "1",
        "TopicName": "test-topic-1",
        "RetentionTimeInMs": "10000",
        "MaxMessageBytes" : "1000012"
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

    def test_topic_signals(self):
        """
        Send multiple CF-signals to CREATE, MODIFY and DELETE the current topic state
        """
        self.send_create_signal()
        self.send_update_signal_with_new_partitioning()
        self.send_update_signal_with_new_cleanup_policy()
        self.send_update_signal_with_new_retention_time()
        self.send_update_signal_with_new_max_message_size()
        self.send_delete_signal()

    def send_create_signal(self):
        """
        Test that topics are successfully created on 'create' signal
        """
        handler(CREATE, MockContext)

        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        data = json.loads(status_request)['Data']

        self.assertEquals(data, {"RetentionTimeInMs": "5000", "Replications": "1", "TopicName": "test-topic-1",
                                 "CleanupPolicy": "compact", "MaxMessageBytes" : "1000012"})
        self.assertEqual(status, "SUCCESS")

    def send_update_signal_with_new_partitioning(self):
        """
        Test that topics are successfully created AND/OR deleted on 'update' signal
        where the paritioning of the topic changed
        """
        handler(UPDATE_1, MockContext)

        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        data = json.loads(status_request)['Data']

        self.assertEquals(data, {"RetentionTimeInMs": "5000", "Replications": "1", "TopicName": "test-topic-1",
                                 "CleanupPolicy": "compact", "MaxMessageBytes" : "1000012"})
        self.assertEqual(status, "SUCCESS")

    def send_update_signal_with_new_cleanup_policy(self):
        """
        Test that topics are successfully created AND/OR deleted on 'update' signal
        where only the cleanup policy changes
        """
        handler(UPDATE_2, MockContext)

        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))

        status = json.loads(status_request)['Status']
        data = json.loads(status_request)['Data']

        # default cleanup policy is returned (== delete)
        self.assertEqual(data, {"Replications": "1", "TopicName": "test-topic-1", "RetentionTimeInMs": "5000",
                                "CleanupPolicy": "delete", "MaxMessageBytes" : "1000012"})
        self.assertEqual(status, "SUCCESS")

    def send_update_signal_with_new_retention_time(self):
        """
        Test that topics are successfully created AND/OR deleted on 'update' signal
        where only the cleanup policy changes
        """
        handler(UPDATE_3, MockContext)

        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        data = json.loads(status_request)['Data']

        self.assertEquals(data, {"RetentionTimeInMs": "10000", "Replications": "1", "TopicName": "test-topic-1",
                                 "CleanupPolicy": "compact", "MaxMessageBytes" : "1000012"})
        self.assertEqual(status, "SUCCESS")

    def send_update_signal_with_new_max_message_size(self):
        """
        Test that the max. allowed message size of the test-topic-1 can be adjusted with an update signal
        """
        handler(UPDATE_4, MockContext)

        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        data = json.loads(status_request)['Data']

        self.assertEquals(data, {"RetentionTimeInMs": "10000", "Replications": "1", "TopicName": "test-topic-1",
                                 "CleanupPolicy": "compact", "MaxMessageBytes" : "20000012"})
        self.assertEqual(status, "SUCCESS")


    def send_delete_signal(self):
        """
        Test that topics are successfully deleted on 'delete' signal
        """
        handler(DELETE, MockContext)

        status_request = httpretty.last_request().body
        logger.info("CF-RESPONSE CALL: " + str(status_request))
        status = json.loads(status_request)['Status']
        data = json.loads(status_request)['Data']

        self.assertEquals(data, {})
        self.assertEqual(status, "SUCCESS")


if __name__ == "__main__":
    unittest.main()
