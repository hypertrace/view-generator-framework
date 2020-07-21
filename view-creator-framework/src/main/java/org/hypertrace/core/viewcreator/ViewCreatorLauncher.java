package org.hypertrace.core.viewcreator;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Parameter;
import java.util.List;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** ViewCreatorLauncher creates Pinot tables as specified in the config. */
public class ViewCreatorLauncher extends PlatformService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewCreatorLauncher.class);
  private static final String TOOL_CLASS = "tool.class";
  private static final String VIEWS = "views";
  private static final String SERVICE_NAME_CONFIG = "service.name";

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
    } catch (ClassNotFoundException
        | IllegalAccessException
        | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected void doStop() {
    String istioPilotQuitEndpoint = "http://127.0.0.1:15020/quitquitquit";
    HttpClient httpclient = HttpClients.createDefault();
    HttpPost httppost = new HttpPost(istioPilotQuitEndpoint);
    try {
      httpclient.execute(httppost);
      LOGGER.info("Request to pilot succeeded");
    } catch (IOException e) {
      LOGGER.error("Error while calling quitquitquit", e);
    }
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  @Override
  public String getServiceName() {
    return getAppConfig().getString(SERVICE_NAME_CONFIG);
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
