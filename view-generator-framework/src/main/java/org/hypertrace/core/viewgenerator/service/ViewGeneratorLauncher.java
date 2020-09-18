package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATOR_CLASS_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewGeneratorLauncher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(ViewGeneratorLauncher.class);
  private static final String DEFAULT_VIEW_GEN_JOB_CONFIG_KEY = "view-gen";
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
  public StreamsBuilder buildTopology(Map<String, Object> properties, StreamsBuilder streamsBuilder,
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
      inputStream = streamsBuilder
          .stream(inputTopic,
              Consumed.with(Serdes.String(), null));
      inputStreams.put(inputTopic, inputStream);
    }

    inputStream
        .flatMapValues(viewMapper)
        .to(outputTopic,
            Produced.with(Serdes.String(), null));

    return streamsBuilder;
  }

  @Override
  public Map<String, Object> getStreamsConfig(Config config) {
    Map<String, Object> properties = new HashMap<>(
        ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));
    return properties;
  }

  @Override
  public String getJobConfigKey() {
    String jobConfigKey = getViewGenName();
    return jobConfigKey != null ? jobConfigKey : DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    Config jobConfig = getJobConfig(properties);
    return Arrays.asList(jobConfig.getString(INPUT_TOPIC_CONFIG_KEY));
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Config jobConfig = getJobConfig(properties);
    return Arrays.asList(jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY));
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }
}
