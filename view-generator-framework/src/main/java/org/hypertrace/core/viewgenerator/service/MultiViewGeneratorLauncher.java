package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.INPUT_TOPICS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.MULTI_VIEW_GEN_JOB_CONFIG;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.VIEW_GENERATORS_CONFIG;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;

public class MultiViewGeneratorLauncher extends KafkaStreamsApp {
  private final Map<String, Config> viewGenConfigs;

  public MultiViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
    viewGenConfigs = new HashMap<>();
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> properties,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    List<String> viewGenNames = getViewGenName(properties);

    for (String viewGenName : viewGenNames) {
      ConfigClient client = ConfigClientFactory.getClient();
      Config viewGenConfig = getSubJobConfig(client, viewGenName);
      viewGenConfigs.put(viewGenName, viewGenConfig);

      // build using its own job config and properties
      ViewGeneratorLauncher viewGenJob = createViewGenJob(client, viewGenName);
      Map<String, Object> viewGenProperties = viewGenJob.getStreamsConfig(viewGenConfig);
      viewGenProperties.put(viewGenJob.getJobConfigKey(), viewGenConfig);
      streamsBuilder = viewGenJob.buildTopology(viewGenProperties, streamsBuilder, inputStreams);

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
    Set<String> inputTopics = new TreeSet<>();
    for (String viewGen : viewGenNames) {
      Config viewGenConfig = viewGenConfigs.get(viewGen);
      inputTopics.addAll(viewGenConfig.getStringList(INPUT_TOPICS_CONFIG_KEY));
    }
    return new ArrayList<>(inputTopics);
  }

  @Override
  public List<String> getOutputTopics(Map<String, Object> properties) {
    List<String> viewGenNames = getViewGenName(properties);
    Set<String> outputTopics = new TreeSet<>();
    for (String viewGen : viewGenNames) {
      Config viewGenConfig = viewGenConfigs.get(viewGen);
      outputTopics.add(viewGenConfig.getString(OUTPUT_TOPIC_CONFIG_KEY));
    }
    return new ArrayList<>(outputTopics);
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
    return client.getConfig(
        jobName,
        ConfigUtils.getEnvironmentProperty("cluster.name"),
        ConfigUtils.getEnvironmentProperty("pod.name"),
        ConfigUtils.getEnvironmentProperty("container.name"));
  }
}
