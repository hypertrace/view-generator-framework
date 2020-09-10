package org.hypertrace.core.viewgenerator.service;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_CLASS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.JOB_CONFIG;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.SCHEMA_REGISTRY_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATOR_CLASS_CONFIG_KEY;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.serdes.SchemaRegistryBasedAvroSerde;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewGeneratorLauncher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(ViewGeneratorLauncher.class);
  private Map<String, String> schemaRegistryConfig;
  private String viewGenName;

  public ViewGeneratorLauncher(ConfigClient configClient, String viewGenName) {
    super(configClient);
    this.viewGenName = viewGenName;
  }

  public String getViewGenName() {
    return viewGenName;
  }

  @Override
  public StreamsBuilder buildTopology(Properties properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {
    String inputClass = properties.getProperty(INPUT_CLASS_CONFIG_KEY);
    Class<?> inputClz = getInputClass(inputClass);

    String viewGeneratorClassName = properties.getProperty(VIEW_GENERATOR_CLASS_CONFIG_KEY);

    InputToViewMapper viewMapper;
    try {
      viewMapper = new InputToViewMapper(viewGeneratorClassName);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Class<?> outputClass = viewMapper.getViewClass();

    SchemaRegistryBasedAvroSerde inputClzSerde = new SchemaRegistryBasedAvroSerde(inputClz);
    inputClzSerde.configure(schemaRegistryConfig, false);

    SchemaRegistryBasedAvroSerde outputClzSerde = new SchemaRegistryBasedAvroSerde(outputClass);
    outputClzSerde.configure(schemaRegistryConfig, false);

    String inputTopic = properties.getProperty(INPUT_TOPIC_CONFIG_KEY);
    String outputTopic = properties.getProperty(OUTPUT_TOPIC_CONFIG_KEY);

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
  public Properties getStreamsConfig(Config config) {
    Properties properties = new Properties();

    schemaRegistryConfig = ConfigUtils.getFlatMapConfig(config, SCHEMA_REGISTRY_CONFIG_KEY);
    properties.putAll(schemaRegistryConfig);

    properties.put(INPUT_TOPIC_CONFIG_KEY, config.getString(INPUT_TOPIC_CONFIG_KEY));
    properties.put(OUTPUT_TOPIC_CONFIG_KEY, config.getString(OUTPUT_TOPIC_CONFIG_KEY));
    properties.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));

    properties.put(VIEW_GENERATOR_CLASS_CONFIG_KEY, config.getString(VIEW_GENERATOR_CLASS_CONFIG_KEY));
    properties.put(INPUT_CLASS_CONFIG_KEY, config.getString(INPUT_CLASS_CONFIG_KEY));

    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UseWallclockTimeOnInvalidTimestamp.class);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    properties.put(JOB_CONFIG, config);

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
      throw new RuntimeException("Failed to get InputClass from flinkSourceConfig", ex);
    }
  }
}
