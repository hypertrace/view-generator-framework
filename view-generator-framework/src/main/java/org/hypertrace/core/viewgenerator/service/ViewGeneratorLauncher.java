package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPICS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.partitioner.AvroFieldValuePartitioner;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewGeneratorLauncher extends KafkaStreamsApp {
  private static final Logger logger = LoggerFactory.getLogger(ViewGeneratorLauncher.class);
  private String viewGenName;

  public ViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
  }

  public String getViewGenName() {
    return viewGenName;
  }

  public void setViewGenName(String viewGenName) {
    this.viewGenName = viewGenName;
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    Config jobConfig = getJobConfig(properties);

    List<String> inputTopics = jobConfig.getStringList(INPUT_TOPICS_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<String, Object> mergedStream = null;

    for (String topic : inputTopics) {
      KStream<String, Object> inputStream = (KStream<String, Object>) inputStreams.get(topic);

      if (inputStream == null) {
        inputStream =
            streamsBuilder.stream(
                topic, Consumed.with(Serdes.String(), null).withName("source-" + topic));
        inputStreams.put(topic, inputStream);

        if (mergedStream == null) {
          mergedStream = inputStream;
        } else {
          mergedStream = mergedStream.merge(inputStream, Named.as("merged-stream"));
        }
      }
    }

    // This environment property helps in overriding producer value serde. For hypertrace quickstart
    // deployment, this helps in using GenericAvroSerde for pinot views.
    Serde producerValueSerde = null;
    String envProducerValueSerdeClassName = System.getenv("PRODUCER_VALUE_SERDE");
    if (envProducerValueSerdeClassName != null) {
      try {
        logger.info("Using producer value serde: {}", envProducerValueSerdeClassName);
        Class clazz = Class.forName(envProducerValueSerdeClassName);
        producerValueSerde = (Serde) clazz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    mergedStream
        .process(() -> new ViewGenerationProcessor(getJobConfigKey()))
        .to(
            outputTopic,
            Produced.with(
                    Serdes.String(), producerValueSerde, getPartitioner(properties, outputTopic))
                .withName("sink-" + outputTopic));
    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    String jobConfigKey = getViewGenName();
    return jobConfigKey != null ? jobConfigKey : DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }

  private AvroFieldValuePartitioner<GenericRecord> getPartitioner(
      Map<String, Object> properties, String topic) {
    String topicsStr = (String) properties.get("avro.field.value.partitioner.enabled.topics");
    if (topicsStr != null) {
      boolean enabledForTopic =
          Splitter.on(",").trimResults().splitToStream(topicsStr).anyMatch(topic::equals);
      if (enabledForTopic) {
        return new AvroFieldValuePartitioner<>(properties);
      }
    }
    return null;
  }
}
