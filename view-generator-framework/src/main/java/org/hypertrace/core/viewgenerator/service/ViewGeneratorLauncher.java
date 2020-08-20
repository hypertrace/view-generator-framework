package org.hypertrace.core.viewgenerator.service;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ViewGeneratorLauncher reads enriched structured traces and converts them to Pinot records using specified Avro
 * schema specified in the target view generator class, which is specified in the config.
 */
public class ViewGeneratorLauncher extends PlatformService {

  private static Logger LOGGER = LoggerFactory.getLogger(ViewGeneratorLauncher.class);

  private ViewGenerationJob job;
  private StreamExecutionEnvironment environment;

  public ViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      environment = FlinkEnvironmentFactory.createExecutionEnv(getAppConfig());
      job = new ViewGenerationJob(environment, getAppConfig());
      job.init();
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ViewGenerationJob.", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStart() {
    try {
      environment.execute(getServiceName());
    } catch (Exception e) {
      LOGGER.error("Got exception during ViewGenerationJob.", e);
      // Since the job couldn't recover from the error state like this.
      // It's better to kill the entire process and let the guardian process to restart the job,
      // e.g. systemd or kubernetes.
      System.exit(1);
    }
  }

  @Override
  protected void doStop() {
    job.stop();
  }

  @Override
  public boolean healthCheck() {
    return true;
  }
}