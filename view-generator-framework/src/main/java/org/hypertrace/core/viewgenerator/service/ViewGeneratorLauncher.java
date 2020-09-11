package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_CLASS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATOR_CLASS_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewGeneratorLauncher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(ViewGeneratorLauncher.class);
  private String viewGenName;
  private Config config;

  public ViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
  }

  public String getViewGenName() {
    return viewGenName;
  }

  public void setViewGenName(String viewGenName) {
    this.viewGenName = viewGenName;
  }

  public Config getConfig() {
    if (config == null) {
      return getAppConfig();
    }
    return config;
  }

  public void setConfig(Config config) {
    this.config = config;
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    Config jobConfig = getConfig();
    String inputClass = jobConfig.getString(INPUT_CLASS_CONFIG_KEY);
    Class<?> inputClz = getInputClass(inputClass);

    String viewGeneratorClassName = jobConfig.getString(VIEW_GENERATOR_CLASS_CONFIG_KEY);

    InputToViewMapper viewMapper;
    try {
      viewMapper = new InputToViewMapper(viewGeneratorClassName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Class<?> outputClass = viewMapper.getViewClass();

    SchemaRegistryBasedAvroSerde inputClzSerde = new SchemaRegistryBasedAvroSerde(inputClz);
    inputClzSerde.configure(properties, false);

    SchemaRegistryBasedAvroSerde outputClzSerde = new SchemaRegistryBasedAvroSerde(outputClass);
    outputClzSerde.configure(properties, false);

    String inputTopic = jobConfig.getString(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<?, ?> inputStream = inputStreams.get(inputTopic);
    if (inputStream == null) {
      inputStream = streamsBuilder
          .stream(inputTopic,
              Consumed.with(Serdes.String(), Serdes.serdeFrom(inputClzSerde, inputClzSerde)));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .flatMapValues(viewMapper)
        .to(outputTopic,
            Produced.with(Serdes.String(), Serdes.serdeFrom(outputClzSerde, outputClzSerde)));

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config config) {
    Map<String, Object> properties = new HashMap<>(
        ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));
    return properties;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  private Class<?> getInputClass(String inputClassName) {
    try {
      return Class.forName(inputClassName);
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load InputClass from job config ", ex);
    }
  }
}
