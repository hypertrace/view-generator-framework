package org.hypertrace.core.viewcreator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.hypertrace.core.viewcreator.pinot.PinotMultiTableCreationTool;
import org.hypertrace.core.viewcreator.pinot.PinotTableCreationTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ViewCreatorLauncher creates Pinot tables as specified in the config.
 */
public class ViewCreatorLauncher {
  private static final Logger LOGGER = LoggerFactory.getLogger(ViewCreatorLauncher.class);

  public static void main(String[] args) {
    updateRuntime();
    launch(args);
  }

  private static void launch(String[] args) {
    String configFilePath = null;
    if (args.length > 0) {
      configFilePath = args[0];
      LOGGER.info("Loading config file path: {}", configFilePath);
    } else {
      LOGGER.error("A config file path is required to launch the job/services.");
      System.exit(1);
    }
    Config fileConfig = ConfigFactory.parseFile(new File(configFilePath));
    Config configs = ConfigFactory.load(fileConfig);

    final String mainClass = configs.getString("mainClass");
    switch (mainClass) {
      case "org.hypertrace.core.viewcreator.pinot.PinotTableCreationTool":
        try {
          ViewCreationSpec viewCreationSpec = ViewCreationSpec
              .parse(configs);
          PinotTableCreationTool tool = new PinotTableCreationTool(viewCreationSpec);
          tool.execute();
        } catch (Exception e) {
          LOGGER.error("Exception while executing PinotTableCreationTool.execute()", e);
          System.exit(1);
        }
        break;
      case "org.hypertrace.core.viewcreator.pinot.PinotMultiTableCreationTool":
        try {
          PinotMultiTableCreationTool tool = new PinotMultiTableCreationTool(configs.getStringList("views"));
          tool.execute();
        } catch (Exception e) {
          LOGGER.error("Exception while executing PinotMultiTableCreationTool.execute()", e);
          System.exit(1);
        }
        break;
      default:
        System.exit(1);
        LOGGER.error("Cannot find the class to execute. Did not create the tables");
    }
  }

  private static void updateRuntime() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      finalizeLauncher();
    }));
  }

  private static void finalizeLauncher() {
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
}
