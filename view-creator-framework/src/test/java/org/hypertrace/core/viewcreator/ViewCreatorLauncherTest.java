package org.hypertrace.core.viewcreator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.viewcreator.pinot.PinotMultiTableCreationTool;
import org.hypertrace.core.viewcreator.pinot.PinotTableCreationTool;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class ViewCreatorLauncherTest {

  @Test
  public void testSingleTableCreationTool() throws Exception {
    Config configs = createConfig("sample-view-generation-spec.conf");

    ConfigClient configClient = Mockito.mock(ConfigClient.class);
    Mockito.when(configClient.getConfig()).thenReturn(configs);

    TableCreationTool creationTool =
        new ViewCreatorLauncher(configClient).getTableCreationTool(configs);
    Assertions.assertNotNull(creationTool);
    Assertions.assertTrue(creationTool instanceof PinotTableCreationTool);
  }

  @Test
  public void testMultiTableCreationTool() throws Exception {
    Config configs = createConfig("all-views.conf");

    ConfigClient configClient = Mockito.mock(ConfigClient.class);
    Mockito.when(configClient.getConfig()).thenReturn(configs);

    TableCreationTool creationTool =
        new ViewCreatorLauncher(configClient).getTableCreationTool(configs);
    Assertions.assertNotNull(creationTool);
    Assertions.assertTrue(creationTool instanceof PinotMultiTableCreationTool);
  }

  private Config createConfig(String resourcePath) {
    File configFile =
        new File(this.getClass().getClassLoader().getResource(resourcePath).getPath());
    return ConfigFactory.parseFile(configFile);
  }
}
