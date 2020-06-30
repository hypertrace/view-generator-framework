package org.hypertrace.core.viewgenerator.service;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MultiViewGeneratorLauncher bundles multiple view generators into single service.
 * Can be used to reduce the number of services/pods/resources.
 * Used in hypertrace standalone mode deployment.
 */
public class MultiViewGeneratorLauncher extends PlatformService {

  private static Logger LOGGER = LoggerFactory.getLogger(MultiViewGeneratorLauncher.class);

  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String VIEW_GENERATORS_CONFIG = "view.generators";

  private String serviceName;
  private List<ViewGenerationJob> viewGenerationJobs;
  private StreamExecutionEnvironment environment;

  public MultiViewGeneratorLauncher(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    try {
      serviceName = getAppConfig().getString(SERVICE_NAME_CONFIG);
      viewGenerationJobs = new ArrayList<>();

      List<String> viewGenNames = getAppConfig().getStringList(VIEW_GENERATORS_CONFIG);
      environment = FlinkEnvironmentFactory.createExecutionEnv(getAppConfig());
      viewGenNames.stream().forEach(viewGen -> {
        viewGenerationJobs.add(initViewGenerator(environment, viewGen));
      });
    } catch (Exception e) {
      LOGGER.error("Failed to initialize ViewGenerationJob.", e);
      throw new RuntimeException(e);
    }
  }

  private ViewGenerationJob initViewGenerator(
      StreamExecutionEnvironment environment,
      String viewGen) {
    ConfigClient configClient = ConfigClientFactory.getClient();
    String cluster = ConfigUtils.getEnvironmentProperty("cluster.name");
    String pod = ConfigUtils.getEnvironmentProperty("pod.name");
    String container = ConfigUtils.getEnvironmentProperty("container.name");

    Config viewGenConfig = configClient.getConfig(viewGen, cluster, pod, container);

    ViewGenerationJob job = new ViewGenerationJob(environment, viewGenConfig);
    job.init();

    return job;
  }

  @Override
  protected void doStart() {
    try {
      environment.execute(serviceName);
    } catch (Exception e) {
      LOGGER.error("Error occurred in view generation", e);
      // Since the job couldn't recover from the error state like this.
      // It's better to kill the entire process and let the guardian process to restart the job,
      // e.g. systemd or kubernetes.
      System.exit(1);
    }
  }

  @Override
  protected void doStop() {
    viewGenerationJobs.stream().forEach(job -> {job.stop();});
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }
}
