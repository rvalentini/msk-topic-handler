class MskConfig:
    """
    Maps AWS-Lambda resource properties to corresponding Kafka settings
    """
    TOPIC_SETTINGS = {
        "CleanupPolicy": {
            "kafkaConfigKey": "cleanup.policy",
            "defaultValue": "delete"
        },
        "RetentionTimeInMs": {
            "kafkaConfigKey": "retention.ms",
            "defaultValue": "604800000"
        },
        "MaxMessageBytes": {
            "kafkaConfigKey": "max.message.bytes",
            "defaultValue": "1000012"
        }
    }

    @staticmethod
    def get_default_setting(setting):
        return setting["defaultValue"] if "defaultValue" in setting else None

    @staticmethod
    def config_from_props(props):
        config = {}
        for setting in MskConfig.TOPIC_SETTINGS:
            val = props.get(setting)
            default = MskConfig.get_default_setting(MskConfig.TOPIC_SETTINGS[setting])
            if val is None and default is not None:
                val = default
            if val is not None:
                config[MskConfig.TOPIC_SETTINGS[setting]["kafkaConfigKey"]] = val
        return config

    @staticmethod
    def props_from_config(config_dict):
        props_dict = {}
        for setting in MskConfig.TOPIC_SETTINGS:
            for key in config_dict:
                if key == MskConfig.TOPIC_SETTINGS[setting]["kafkaConfigKey"]:
                    props_dict[setting] = config_dict[key]
        return props_dict
