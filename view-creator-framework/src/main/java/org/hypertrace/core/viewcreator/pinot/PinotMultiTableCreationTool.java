package org.hypertrace.core.viewcreator.pinot;

import com.typesafe.config.Config;
import java.util.List;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.viewcreator.TableCreationTool;
import org.hypertrace.core.viewcreator.ViewCreationSpec;

public class PinotMultiTableCreationTool implements TableCreationTool {
  private static final String CLUSTER_NAME = "cluster.name";
  private static final String POD_NAME = "pod.name";
  private static final String CONTAINER_NAME = "container.name";
  private final List<String> viewsToCreate;

  public PinotMultiTableCreationTool(List<String> viewsToCreate) {
    this.viewsToCreate = viewsToCreate;
  }

  @Override
  public void create() {
    viewsToCreate.forEach(
        viewName -> {
          ViewCreationSpec viewCreationSpec = parseViewCreationSpec(viewName);
          PinotTableCreationTool tableCreationTool = new PinotTableCreationTool(viewCreationSpec);
          tableCreationTool.create();
        });
  }

  private ViewCreationSpec parseViewCreationSpec(String viewName) {
    ConfigClient configClient = ConfigClientFactory.getClient();
    String cluster = ConfigUtils.getEnvironmentProperty(CLUSTER_NAME);
    String pod = ConfigUtils.getEnvironmentProperty(POD_NAME);
    String container = ConfigUtils.getEnvironmentProperty(CONTAINER_NAME);

    Config viewConfig = configClient.getConfig(viewName, cluster, pod, container);
    return ViewCreationSpec.parse(viewConfig);
  }
}
