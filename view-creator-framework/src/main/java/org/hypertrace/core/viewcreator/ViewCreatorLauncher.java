package org.hypertrace.core.viewcreator;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.List;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ViewCreatorLauncher creates Pinot tables as specified in the config. */
public class ViewCreatorLauncher extends PlatformService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewCreatorLauncher.class);
  private static final String TOOL_CLASS = "tool.class";
  private static final String VIEWS = "views";

  public ViewCreatorLauncher(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {}

  @Override
  protected void doStart() {
    try {
      getTableCreationTool(getAppConfig()).create();
      shutdown();
    } catch (Exception e) {
      LOGGER.error("Error in doStart", e);
      // Important to exit(1) so that the guardian process(eg k8s or systemd) can restart the job
      System.exit(1);
    }
  }

  @Override
  protected void doStop() {}

  @Override
  public boolean healthCheck() {
    return true;
  }

  @VisibleForTesting
  TableCreationTool getTableCreationTool(Config config)
      throws ClassNotFoundException, IllegalAccessException, InvocationTargetException,
          InstantiationException {
    Class clz = Class.forName(getAppConfig().getString(TOOL_CLASS));
    if (!TableCreationTool.class.isAssignableFrom(clz)) {
      throw new ClassNotFoundException("class is not table creation tool");
    }

    for (Constructor ctor : clz.getConstructors()) {
      Parameter[] parameters = ctor.getParameters();
      if (parameters != null && parameters.length == 1) {
        if (ViewCreationSpec.class.equals(parameters[0].getType())) {
          ViewCreationSpec viewCreationSpec = ViewCreationSpec.parse(config);
          return (TableCreationTool) ctor.newInstance(viewCreationSpec);
        } else if (List.class.equals(parameters[0].getType())) {
          List<String> views = config.getStringList(VIEWS);
          return (TableCreationTool) ctor.newInstance(views);
        }
      }
    }

    throw new ClassNotFoundException("class doesn't have matching constructor");
  }
}
