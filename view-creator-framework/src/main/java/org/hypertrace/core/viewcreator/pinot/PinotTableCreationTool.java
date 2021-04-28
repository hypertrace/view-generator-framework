package org.hypertrace.core.viewcreator.pinot;

import static org.apache.pinot.spi.config.table.TableType.OFFLINE;
import static org.apache.pinot.spi.config.table.TableType.REALTIME;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.buildPinotTableConfig;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.createPinotSchemaForView;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotOfflineTableSpec;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotRealtimeTableSpec;
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
    final PinotTableSpec realtimeTableSpec = getPinotRealtimeTableSpec(viewCreationSpec);

    final Schema pinotSchemaForView = createPinotSchemaForView(viewCreationSpec, realtimeTableSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchemaForView);
    final boolean uploadPinotSchema = uploadPinotSchema(realtimeTableSpec, pinotSchemaForView);
    if (!uploadPinotSchema) {
      throw new RuntimeException(
          "Failed to upload pinot schema: " + pinotSchemaForView.getSchemaName());
    }

    final TableConfig realtimeTableConfig =
        buildPinotTableConfig(viewCreationSpec, realtimeTableSpec, REALTIME);
    if (!sendPinotTableCreationRequest(realtimeTableSpec, realtimeTableConfig)) {
      throw new RuntimeException(
          "Failed to create pinot realtime table: " + viewCreationSpec.getViewName());
    }

    final PinotTableSpec offlineTableSpec = getPinotOfflineTableSpec(viewCreationSpec);
    final TableConfig offlineTableConfig =
        buildPinotTableConfig(viewCreationSpec, offlineTableSpec, OFFLINE);
    if (!sendPinotTableCreationRequest(offlineTableSpec, offlineTableConfig)) {
      throw new RuntimeException(
          "Failed to create pinot offline table: " + viewCreationSpec.getViewName());
    }
  }
}
