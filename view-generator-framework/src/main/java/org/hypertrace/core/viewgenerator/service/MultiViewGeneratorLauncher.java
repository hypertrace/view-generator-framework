package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.JOB_CONFIG;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATORS_CONFIG;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.timestampextractors.UseWallclockTimeOnInvalidTimestamp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiViewGeneratorLauncher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(MultiViewGeneratorLauncher.class);

  private List<ViewGeneratorLauncher> viewGeneratorLaunchers;
  private Map<String, Config> viewGenConfigs = new HashMap<>();

  public MultiViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
    viewGeneratorLaunchers = new ArrayList<>();
    viewGenConfigs = new HashMap<>();
  }

  @Override
  public StreamsBuilder buildTopology(Properties properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> map) {
    List<String> viewGenNames = (List<String>) properties.get(VIEW_GENERATORS_CONFIG);
    if (viewGeneratorLaunchers == null) {
      viewGeneratorLaunchers = new ArrayList<>();
    }
    if (viewGenConfigs == null) {
      viewGenConfigs = new HashMap<>();
    }
    viewGenNames.stream().forEach(viewGen -> {
      viewGeneratorLaunchers.add(createViewGenJob(viewGen));
    });
    for (ViewGeneratorLauncher job : viewGeneratorLaunchers) {
      Config config = viewGenConfigs.get(job.getViewGenName());
      Properties props = job.getStreamsConfig(config);
      streamsBuilder = job.buildTopology(props, streamsBuilder, map);
    }

    return streamsBuilder;
  }

  @Override
  public Properties getStreamsConfig(Config config) {
    Properties properties = new Properties();
    properties.putAll(ConfigUtils.getFlatMapConfig(config, KAFKA_STREAMS_CONFIG_KEY));

    properties.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
        UseWallclockTimeOnInvalidTimestamp.class);
    properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        LogAndContinueExceptionHandler.class);

    properties.put(VIEW_GENERATORS_CONFIG, config.getStringList(VIEW_GENERATORS_CONFIG));
    properties.put(JOB_CONFIG, config);

    return properties;
  }

  @Override
  public Logger getLogger() {
    return logger;
  }

  private ViewGeneratorLauncher createViewGenJob(String viewGen) {
    ConfigClient configClient = ConfigClientFactory.getClient();
    String cluster = ConfigUtils.getEnvironmentProperty("cluster.name");
    String pod = ConfigUtils.getEnvironmentProperty("pod.name");
    String container = ConfigUtils.getEnvironmentProperty("container.name");

    Config viewGenConfig = configClient.getConfig(viewGen, cluster, pod, container);
    viewGenConfigs.put(viewGen, viewGenConfig);
    return new ViewGeneratorLauncher(configClient, viewGen);
  }
}
