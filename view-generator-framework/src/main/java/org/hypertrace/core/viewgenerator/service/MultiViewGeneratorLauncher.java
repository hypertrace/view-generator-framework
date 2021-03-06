package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATORS_CONFIG;

import com.typesafe.config.Config;
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

public class MultiViewGeneratorLauncher extends KafkaStreamsApp {

  private static final String MULTI_VIEW_GEN_JOB_CONFIG = "multi-view-gen-job-config";

  private Map<String, Config> viewGenConfigs;

  public MultiViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
    viewGenConfigs = new HashMap<>();
  }

  @Override
  public StreamsBuilder buildTopology(Map<String, Object> properties, StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> map) {

    List<String> viewGenNames = getViewGenName(properties);

    for (String viewGen : viewGenNames) {
      ConfigClient client = ConfigClientFactory.getClient();
      Config viewGenConfig = getSubJobConfig(client, viewGen);
      viewGenConfigs.put(viewGen, viewGenConfig);

      // build using its own job config and properties
      ViewGeneratorLauncher viewGenJob = createViewGenJob(client, viewGen);
      Map<String, Object> viewGenProperties = viewGenJob.getStreamsConfig(viewGenConfig);
      viewGenProperties.put(viewGenJob.getJobConfigKey(), viewGenConfig);
      streamsBuilder = viewGenJob.buildTopology(viewGenProperties, streamsBuilder, map);

      // retains the job specific config in main properties which is passed as context if need be.
      properties.put(viewGenJob.getJobConfigKey(), viewGenConfig);
    }

    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    return MULTI_VIEW_GEN_JOB_CONFIG;
  }

  @Override
  public List<String> getInputTopics(Map<String, Object> properties) {
    List<String> viewGenNames = getViewGenName(properties);
    Set<String> inputTopics = new HashSet<>();
    for (String viewGen : viewGenNames) {
      Config viewGenConfig = viewGenConfigs.get(viewGen);
      inputTopics.add(viewGenConfig.getString(INPUT_TOPIC_CONFIG_KEY));
    }
    return inputTopics.stream().collect(Collectors.toList());
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    List<String> viewGenNames = getViewGenName(properties);
    Set<String> outputTopics = new HashSet<>();
    for (String viewGen : viewGenNames) {
      Config viewGenConfig = viewGenConfigs.get(viewGen);
      outputTopics.add(viewGenConfig.getString(OUTPUT_TOPIC_CONFIG_KEY));
    }
    return outputTopics.stream().collect(Collectors.toList());
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }

  private List<String> getViewGenName(Map<String, Object> properties) {
    return getJobConfig(properties).getStringList(VIEW_GENERATORS_CONFIG);
  }

  private ViewGeneratorLauncher createViewGenJob(ConfigClient client, String viewGen) {
    ViewGeneratorLauncher viewGeneratorLauncher = new ViewGeneratorLauncher(client);
    viewGeneratorLauncher.setViewGenName(viewGen);
    return viewGeneratorLauncher;
  }

  private Config getSubJobConfig(ConfigClient client, String jobName) {
    return client.getConfig(jobName,
        ConfigUtils.getEnvironmentProperty("cluster.name"),
        ConfigUtils.getEnvironmentProperty("pod.name"),
        ConfigUtils.getEnvironmentProperty("container.name")
    );
  }
}
