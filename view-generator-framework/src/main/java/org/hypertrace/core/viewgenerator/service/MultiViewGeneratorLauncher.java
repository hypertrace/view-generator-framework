package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.KAFKA_STREAMS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATORS_CONFIG;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiViewGeneratorLauncher extends KafkaStreamsApp {

  private static final Logger logger = LoggerFactory.getLogger(MultiViewGeneratorLauncher.class);
  private static final String MULTI_VIEW_GEN_JOB_CONFIG = "multi-view-gen-job-config";

  private final List<ViewGeneratorLauncher> viewGeneratorLaunchers;
  private final Map<String, Config> viewGenConfigs;

  public MultiViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
    viewGeneratorLaunchers = new ArrayList<>();
    viewGenConfigs = new HashMap<>();
    List<String> viewGenNames = getAppConfig().getStringList(VIEW_GENERATORS_CONFIG);
    viewGenNames.forEach(viewGen -> {
      viewGeneratorLaunchers.add(createViewGenJob(viewGen));
    });
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> map) {

    for (ViewGeneratorLauncher job : viewGeneratorLaunchers) {
      Config config = viewGenConfigs.get(job.getViewGenName());
      Map<String, Object> props = job.getStreamsConfig(config);
      props.put(job.getViewGenName(), config);
      streamsBuilder = job.buildTopology(props, streamsBuilder, map);
      properties.put(job.getViewGenName(), config);
    }

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

  @Override
  public String getJobConfigKey() {
    return MULTI_VIEW_GEN_JOB_CONFIG;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    Set<String> inputTopics = new HashSet<>();
    for (ViewGeneratorLauncher job : viewGeneratorLaunchers) {
      inputTopics.add(viewGenConfigs.get(job.getViewGenName()).getString(INPUT_TOPIC_CONFIG_KEY));
    }
    return inputTopics.stream().collect(Collectors.toList());
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    Set<String> outputTopics = new HashSet<>();
    for (ViewGeneratorLauncher job : viewGeneratorLaunchers) {
      outputTopics.add(viewGenConfigs.get(job.getViewGenName()).getString(OUTPUT_TOPIC_CONFIG_KEY));
    }
    return outputTopics.stream().collect(Collectors.toList());
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }

  private ViewGeneratorLauncher createViewGenJob(String viewGen) {
    ConfigClient configClient = ConfigClientFactory.getClient();
    String cluster = ConfigUtils.getEnvironmentProperty("cluster.name");
    String pod = ConfigUtils.getEnvironmentProperty("pod.name");
    String container = ConfigUtils.getEnvironmentProperty("container.name");

    Config viewGenConfig = configClient.getConfig(viewGen, cluster, pod, container);
    viewGenConfigs.put(viewGen, viewGenConfig);
    ViewGeneratorLauncher viewGeneratorLauncher = new ViewGeneratorLauncher(configClient);
    viewGeneratorLauncher.setViewGenName(viewGen);
    viewGeneratorLauncher.setConfig(viewGenConfig);
    return viewGeneratorLauncher;
  }
}
