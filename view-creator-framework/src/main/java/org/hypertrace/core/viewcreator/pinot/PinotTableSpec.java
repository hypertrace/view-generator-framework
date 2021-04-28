package org.hypertrace.core.viewcreator.pinot;

import com.typesafe.config.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Data;

@Data
public class PinotTableSpec {

  // Cluster configs
  private String controllerHost;
  private String controllerPort;

  // Table configs;
  private String tableName;
  private String loadMode;

  private List<String> dimensionColumns;
  private List<String> metricColumns;
  @Optional private List<String> dateTimeColumns;

  // Table index configs
  private List<String> invertedIndexColumns;
  private List<String> noDictionaryColumns;
  private List<String> bloomFilterColumns;
  private List<String> rangeIndexColumns;
  private Map<String, Object> columnsMaxLength;

  // Segments config
  private int numReplicas;
  private TimeUnit timeUnit;
  private String timeColumn;
  private String retentionTimeValue;
  private String retentionTimeUnit;
  private String segmentAssignmentStrategy;

  // Tenants config
  private String brokerTenant;
  private String serverTenant;

  public PinotTableSpec() {}
}
