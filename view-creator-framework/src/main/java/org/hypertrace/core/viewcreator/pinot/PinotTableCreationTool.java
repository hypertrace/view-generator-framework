package org.hypertrace.core.viewcreator.pinot;

import static org.hypertrace.core.viewcreator.pinot.PinotUtils.createPinotSchemaForView;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.buildRealTimeTableConfig;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotRealTimeTableSpec;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.sendPinotTableCreationRequest;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.uploadPinotSchema;

import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.hypertrace.core.viewcreator.TableCreationTool;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotTableCreationTool implements TableCreationTool {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotTableCreationTool.class);

  private final ViewCreationSpec viewCreationSpec;

  public PinotTableCreationTool(ViewCreationSpec viewCreationSpec) {
    this.viewCreationSpec = viewCreationSpec;
  }

  @Override
  public void create() {
    final PinotRealtimeTableSpec realtimeTableSpec = getPinotRealTimeTableSpec(viewCreationSpec);
    final Schema pinotSchemaForView = createPinotSchemaForView(viewCreationSpec, realtimeTableSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchemaForView);
    final boolean uploadPinotSchema = uploadPinotSchema(realtimeTableSpec, pinotSchemaForView);
    if (!uploadPinotSchema) {
      throw new RuntimeException("Failed to upload pinot schema.");
    }
    final TableConfig tableConfig = buildRealTimeTableConfig(viewCreationSpec, realtimeTableSpec);
    final boolean createPinotTable = sendPinotTableCreationRequest(viewCreationSpec, tableConfig);
    if (!createPinotTable) {
      throw new RuntimeException("Failed to create pinot table.");
    }
  }
}
