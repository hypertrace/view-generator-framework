package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATOR_CLASS_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.Map;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewGeneratorLauncher extends KafkaStreamsApp {
  private static final Logger logger = LoggerFactory.getLogger(ViewGeneratorLauncher.class);

  private static final String DEFAULT_VIEW_GEN_JOB_CONFIG_KEY = "view-gen-job-config-key";
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
    String viewGeneratorClassName = jobConfig.getString(VIEW_GENERATOR_CLASS_CONFIG_KEY);

    InputToViewMapper viewMapper;
    try {
      viewMapper = new InputToViewMapper(viewGeneratorClassName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    String inputTopic = jobConfig.getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<?, ?> inputStream = inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder.stream(inputTopic, Consumed.with(Serdes.String(), null));
      inputStreams.put(inputTopic, inputStream);
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

    inputStream
        .flatMapValues(viewMapper)
        .to(outputTopic, Produced.with(Serdes.String(), producerValueSerde));
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
}
