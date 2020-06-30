package org.hypertrace.core.viewcreator.pinot;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.hypertrace.core.viewcreator.ViewCreationSpec;

public class PinotMultiTableCreationTool {
  private List<String> viewsToCreate;

  public PinotMultiTableCreationTool(List<String> viewsToCreate) {
    this.viewsToCreate = viewsToCreate;
  }

  public void execute() {
    viewsToCreate.stream().forEach(viewName -> {
      ViewCreationSpec viewCreationSpec = parseViewCreationSpec(viewName);
      PinotTableCreationTool tableCreationTool = new PinotTableCreationTool(viewCreationSpec);
      tableCreationTool.execute();
    });
  }

  private ViewCreationSpec parseViewCreationSpec(String viewName) {
    Config viewConfig = ConfigFactory.parseResources(String.format("configs/%s/job.conf", viewName));
    return ViewCreationSpec.parse(viewConfig);
  }
}
