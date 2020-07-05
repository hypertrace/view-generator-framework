package org.hypertrace.core.viewcreator.pinot;

import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotTableCreationTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableCreationTool.class);

  private final ViewCreationSpec viewCreationSpec;

  public PinotTableCreationTool(ViewCreationSpec viewCreationSpec) {
    this.viewCreationSpec = viewCreationSpec;
  }

  public void execute() {
    final Schema pinotSchemaForView = PinotUtils.createPinotSchemaForView(viewCreationSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchemaForView);
    final boolean uploadPinotSchema = PinotUtils
        .uploadPinotSchema(viewCreationSpec, pinotSchemaForView);
    if (!uploadPinotSchema) {
      throw new RuntimeException("Failed to upload Pinot schema.");
    }
    final TableConfig tableConfig = PinotUtils.createPinotTable(viewCreationSpec);
    final boolean createPinotTable = PinotUtils
        .sendPinotTableCreationRequest(viewCreationSpec, tableConfig);
    if (!createPinotTable) {
      throw new RuntimeException("Failed to create Pinot table.");
    }
  }
}