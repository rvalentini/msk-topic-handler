from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource
from kafka import KafkaConsumer
import logging
import time


class MskService:
    # Can be "PLAINTEXT" or "SSL"
    SECURITY_PROTOCOL = "SSL"
    logging.basicConfig()
    logger = logging.getLogger(__name__)

    def __init__(self, bootstrap_servers, client_id):
        MskService.logger.info("Initializing MSK service")
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers,
                                             client_id=client_id,
                                             security_protocol=MskService.SECURITY_PROTOCOL)
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      security_protocol=MskService.SECURITY_PROTOCOL)

    def create_topic(self, name, partitions, replication, config):
        MskService.logger.info(
            "CREATE topic called with args: " + str(name) + " , " + str(partitions) + " , " + str(replication))
        current_topics = self.consumer.topics()
        if name not in current_topics:
            topic = NewTopic(name=name, num_partitions=partitions, replication_factor=replication, topic_configs=config)
            return self.admin_client.create_topics([topic], validate_only=False)
        else:
            MskService.logger.info("Topic '" + str(name) + "' already exisits! Doing nothing ...")
            return []

    def update_topic(self, new_name, old_name, new_partitions, new_replications, old_replications, new_config,
                     old_config):
        MskService.logger.info("------ Updating MSK topic " + str(new_name) + " ------")
        current_topics = self.consumer.topics()
        if new_name in current_topics:
            # topic exists
            old_partitions = len(self.consumer.partitions_for_topic(new_name))
            if self.has_topic_changed(old_partitions, new_partitions, old_replications, new_replications, old_config,
                                      new_config):
                MskService.logger.info("Topic configuration changed!")
                # topic config changed, but has still the same name
                self.delete_topic(new_name)
                time.sleep(1)
                return self.create_topic(new_name, new_partitions, new_replications, new_config)
            else:
                # topic not changed -> fine do nothing
                MskService.logger.info("No change detected for topic!")
        else:
            # topic does not exist
            MskService.logger.info("Topic was newly added/renamed -> DELETE original topic and CREATE new topic")
            # try to delete dereferenced topic and create new topic
            self.delete_topic(old_name)
            return self.create_topic(new_name, new_partitions, new_replications, new_config)
        return []

    def delete_topic(self, name):
        MskService.logger.info("DELETE topic called")
        current_topics = self.consumer.topics()
        if name in current_topics:
            return self.admin_client.delete_topics([name])
        else:
            MskService.logger.info("Topic '" + str(name) + "' does not exist! Doing nothing ... ")
            return []

    @staticmethod
    def has_topic_changed(old_partitions, new_partitions, old_replications, new_replications, old_config,
                          new_config):
        # has the replication factor changed?
        replica_changed = new_replications != old_replications
        # have the number of partitions changed?
        partitions_changed = new_partitions != old_partitions
        # has the config changed?
        config_changed = old_config != new_config
        MskService.logger.debug("PARTITIONS: OLD " + str(old_partitions) + " NEW: " + str(new_partitions))
        MskService.logger.debug("REPLICATIONS: OLD " + str(old_replications) + " NEW: " + str(new_replications))
        MskService.logger.debug("CONFIG OLD " + str(old_config) + "NEW: " + str(new_config))
        return replica_changed or partitions_changed or config_changed
