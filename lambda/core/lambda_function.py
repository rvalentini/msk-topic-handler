from crhelper import CfnResource
from msk_service import MskService
from msk_config import MskConfig
import logging

logging.basicConfig()
logger = logging.getLogger(__name__)
helper = CfnResource(json_logging=False, log_level='INFO', boto_level='CRITICAL')


@helper.create
def create(event, _):
    logger.info("CREATE signal received with event" + str(event))

    # initialize msk service
    msk_service = MskService(event['ResourceProperties']['BootstrapServers'], event['ResourceProperties']['ClientId'])
    # load topic information from input
    name = event['ResourceProperties']['TopicName']
    partitions = event['ResourceProperties']['Partitions']
    replications = event['ResourceProperties']['Replications']

    config = MskConfig.config_from_props(event['ResourceProperties'])
    result = msk_service.create_topic(name,
                                     int(partitions),
                                     int(replications),
                                     config)

    # add replication for topic to cf-state for later updates
    persist_replication_state(name, replications)
    logger.info('Result of create topic operation: ' + str(result))

    # add original topic names to cf-state for later updates
    persist_original_topic_name(name)

    # add config values to cf-state for later updates
    persist_original_config(name, config)


@helper.update
def update(event, _):
    logger.info("UPDATE signal received with event" + str(event))
    # If the update resulted in a new resource being created, return an id for the new resource. 
    # CloudFormation will send a delete event with the old id when stack update completes

    # initialize msk service
    msk_service = MskService(event['ResourceProperties']['BootstrapServers'], event['ResourceProperties']['ClientId'])
    # load topic information from input
    name = event['ResourceProperties']['TopicName']
    partitions = event['ResourceProperties']['Partitions']
    replications = event['ResourceProperties']['Replications']
    config = MskConfig.config_from_props(event['ResourceProperties'])

    # retrieve old topic_names from stack data
    old_name = event['OldResourceProperties']['TopicName']
    # load old replication value from stack data if exists

    if 'Replications' in event['OldResourceProperties']:
        old_replications = event['OldResourceProperties']['Replications']
    else:
        old_replications = replications

    old_config = MskConfig.config_from_props(event['OldResourceProperties'])

    result = msk_service.update_topic(name,
                                     old_name,
                                     int(partitions),
                                     int(replications),
                                     int(old_replications),
                                     config,
                                     old_config)
    logger.info('Result of create topic operation: ' + str(result))

    # add replication for topic to cf-state for later updates
    persist_replication_state(name, replications)
    logger.info('Result of create topic operation: ' + str(result))

    # add original topic names to cf-state for later updates
    persist_original_topic_name(name)

    # add config values to cf-state for later updates
    persist_original_config(name, config)


@helper.delete
def delete(event, _):
    logger.info("DELETE signal received with event" + str(event))

    # initialize msk service
    msk_service = MskService(event['ResourceProperties']['BootstrapServers'], event['ResourceProperties']['ClientId'])
    # load topic information from input
    name = event['ResourceProperties']['TopicName']

    result = msk_service.delete_topic(name)
    logger.info('Result of delete topic operation: ' + str(result))


def handler(event, context):
    helper(event, context)


def persist_replication_state(topic_name, replication):
    helper.Data.update({'Replications': replication})
    logger.info('Persisted replication state of topic ' + str(topic_name) + ' to CF-stack')


def persist_original_topic_name(topic_name):
    helper.Data.update({'TopicName': topic_name})
    logger.info('Persisted TopicName to CF-stack: ' + str(topic_name))


def persist_original_config(topic_name, config):
    helper.Data.update(MskConfig.props_from_config(config))
    logger.info('Persisted config of topic ' + str(topic_name) + ' to CF-stack')
