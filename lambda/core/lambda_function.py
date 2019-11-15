from __future__ import print_function
from crhelper import CfnResource
from msk_service import create_topic, delete_topic, update_topic, init
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=False, log_level='INFO', boto_level='CRITICAL')

try:
    ## Init code goes here
    pass
except Exception as e:
    helper.init_failure(e)


@helper.create
def create(event, context):
    logger.info("CREATE signal received with event" + str(event))

    # initialize msk service
    init(event['ResourceProperties']['BootstrapServers'], event['ResourceProperties']['ClientId'])
    # load topic information from input
    name = event['ResourceProperties']['TopicName'] 
    partitions = event['ResourceProperties']['Partitions']
    replications = event['ResourceProperties']['Replications']

    result = create_topic(name,
                         int(partitions),
                         int(replications))
        
        
    # add replication for topic to cf-state for later updates
    persist_replication_state(name, replications)
    logger.info('Result of create topic operation: ' + str(result))

    # add original topic names to cf-state for later updates
    persist_original_topic_name(name)




@helper.update
def update(event, context):
    logger.info("UPDATE signal received with event" + str(event))
    # If the update resulted in a new resource being created, return an id for the new resource. 
    # CloudFormation will send a delete event with the old id when stack update completes

    # initialize msk service
    init(event['ResourceProperties']['BootstrapServers'], event['ResourceProperties']['ClientId'])
    # load topic information from input
    name = event['ResourceProperties']['TopicName'] 
    partitions = event['ResourceProperties']['Partitions']
    replications = event['ResourceProperties']['Replications']

    # retrieve old topic_names from stack data
    old_name = event['OldResourceProperties']['TopicName']
    #load old replication value from stack data if exists
    
    if 'Replications' in event['OldResourceProperties']: 
        old_replications = event['OldResourceProperties']['Replications']
    else:
        old_replications = replications
    
    result = update_topic(name,
                old_name,
                int(partitions),
                int(replications),
                int(old_replications))
    logger.info('Result of create topic operation: ' + str(result))



@helper.delete
def delete(event, context):
    logger.info("DELETE signal received with event" + str(event))

    # initialize msk service
    init(event['ResourceProperties']['BootstrapServers'], event['ResourceProperties']['ClientId'])
    # load topic information from input
    name = event['ResourceProperties']['TopicName'] 
     
    result = delete_topic(name)
    logger.info('Result of delete topic operation: ' + str(result))
        


def handler(event, context):
    helper(event, context)


def persist_replication_state(topic_name, replication):
    helper.Data.update({ topic_name : replication})
    logger.info('Persisted replication state of topic ' + str(topic_name) + ' to CF-stack')

def persist_original_topic_name(topic_name):
    helper.Data.update({ 'TopicName' : topic_name })
    logger.info('Persisted TopicName to CF-stack: ' + str(topic_name))