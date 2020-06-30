package org.hypertrace.core.viewcreator.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreConfig;
import org.hypertrace.core.eventstore.kafka.KafkaEventStore;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);
  private static final String KAFKA_CONFIGS_KEY = "kafka";

  public static ViewCreationSpec.KafkaSpec getKafkaSpecFromViewGenerationSpec(ViewCreationSpec spec) {
    try {
      final ViewCreationSpec.KafkaSpec kafkaSpec = ConfigBeanFactory
          .create(spec.getViewGeneratorConfig().getConfig(KAFKA_CONFIGS_KEY), ViewCreationSpec.KafkaSpec.class);
      return kafkaSpec;
    } catch (Exception e) {
      // Assuming the Kafka topic creation is already handled, KafkaSpec is not set.
      LOGGER.info("No valid KafkaSpec found from ViewCreationSpec, skip it.");
      return null;
    }
  }


  /**
   * Create the Kafka topic
   */
  public static boolean createKafkaTopic(ViewCreationSpec.KafkaSpec kafkaSpec) {
    EventStore kafkaEventStore = new KafkaEventStore();
    Map<String, Object> configMap = new HashMap<>();
    configMap.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaSpec.getBrokerAddress());
    EventStoreConfig eventStoreConfig = new EventStoreConfig(ConfigFactory.parseMap(configMap));
    kafkaEventStore.init(eventStoreConfig);
    final List<String> createdTopics = kafkaEventStore.listTopics();
    if (createdTopics.contains(kafkaSpec.getTopicName())) {
      LOGGER.info("Kafka topic: {} is already created, skip topic creation process...",
          kafkaSpec.getTopicName());
      return true;
    }
    LOGGER.info(
        "Trying to create Kafka topic: name - [{}], partitions - [{}], replicationFactor - [{}] on Kafka cluster: [{}].",
        kafkaSpec.getTopicName(), kafkaSpec.getPartitions(), kafkaSpec.getReplicationFactor(),
        kafkaSpec.getBrokerAddress());
    return kafkaEventStore.createTopic(kafkaSpec.getTopicName(),
        getCreateTopicParams(kafkaSpec.getPartitions(), kafkaSpec.getReplicationFactor()));
  }

  private static Config getCreateTopicParams(Integer numPartitions, Integer replicationFactor) {
    Map<String, String> topicParamsMap = new HashMap<>();
    topicParamsMap.put("numPartitions", numPartitions.toString());
    topicParamsMap.put("replicationFactor", replicationFactor.toString());
    return ConfigFactory.parseMap(topicParamsMap);
  }
}
