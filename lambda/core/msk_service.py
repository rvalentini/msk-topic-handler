from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource
from kafka import KafkaConsumer
from kafka.cluster import ClusterMetadata
import logging
import time

logging.basicConfig()
logger = logging.getLogger(__name__)


def init(bootstrap_servers, client_id):
    logger.info("Initializing MSK service")
    global admin_client 
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, 
                                    client_id=client_id,
                                    security_protocol="SSL")  #TODO make SSL configurable
    global consumer 
    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                             security_protocol="SSL")  #TODO make SSL configurable


def create_topic(name, partitions, replication):
    logger.info("CREATE topic called with args: " + str(name) + " , " + str(partitions) + " , " + str(replication))
    current_topics = consumer.topics()
    if name not in current_topics:
        topic = NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
        return admin_client.create_topics([topic], validate_only=False)    
    else:
        logger.info("Topic '" + str(name) + "' already exisits! Doing nothing ...")
        return []


def update_topic(new_name, old_name, new_partitions, new_replications, old_replications):
    logger.info("------ Updating MSK topic " + str(new_name) + " ------" )
    current_topics = consumer.topics()
    if new_name in current_topics:
        #topic exists
        current_partitions = len(consumer.partitions_for_topic(new_name))
        if has_topic_changed(current_partitions, new_partitions, new_replications, old_replications):
            logger.info("Topic configuration changed!")
            # topic config changed, but has still the same name
            delete_topic(new_name)
            time.sleep(1)
            return create_topic(new_name, new_partitions, new_replications)
        else:
            # topic not changed -> fine do nothing
            logger.info("No change detected for topic!")
    else:
        # topic does not exist
        logger.info("Topic was newly added/renamed -> DELETE original topic and CREATE new topic")
        # try to delete dereferenced topic and create new topic
        delete_topic(old_name)
        return create_topic(new_name, new_partitions, new_replications)
    return []



def delete_topic(name):
    logger.info("DELETE topic called")
    current_topics = consumer.topics()
    if name in current_topics:
        return admin_client.delete_topics([name])
    else:
        logger.info("Topic '" + str(name) + "' does not exist! Doing nothing ... ")
        return []



def has_topic_changed(old_partitions, new_partitions, new_replications, old_replications):
    # has the replication factor changed?
    replica_changed = new_replications != old_replications
    # have the number of partitions changed?
    partitions_changed = new_partitions != old_partitions
    logger.debug("PARTITIONS: OLD " + str(old_partitions) + " NEW: " + str(new_partitions) )
    logger.debug("REPLICATIONS: OLD " + str(old_replications) + " NEW: " + str(new_replications) )
    return replica_changed or partitions_changed

