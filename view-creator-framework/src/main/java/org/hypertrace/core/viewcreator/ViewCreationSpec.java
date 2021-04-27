package org.hypertrace.core.viewcreator;

import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class ViewCreationSpec {
  private static final String VIEW_NAME_CONFIG = "view.name";
  private static final String VIEW_OUTPUT_SCHEMA_CLASS_CONFIG = "view.output.schema.class";

  private final String viewName;
  private final String viewOutputSchemaClassName;
  private final Config viewGeneratorConfig;

  public ViewCreationSpec(String viewName, String viewOutputSchemaClassName,
                          Config viewGeneratorConfig) {
    this.viewName = viewName;
    this.viewOutputSchemaClassName = viewOutputSchemaClassName;
    this.viewGeneratorConfig = viewGeneratorConfig;
  }

  public static ViewCreationSpec parse(Config configs) {
    return new ViewCreationSpec(
        configs.getString(VIEW_NAME_CONFIG),
        configs.getString(VIEW_OUTPUT_SCHEMA_CLASS_CONFIG),
        configs
    );
  }

  public String getViewName() {
    return viewName;
  }

  public Config getViewGeneratorConfig() {
    return viewGeneratorConfig;
  }

  // Best effort to extract output schema of a JAVA_CODE based ViewGenerator.
  // This method requires a public constructor and getSchema() method is required to be
  // implemented without other runtime dependencies.
  public Schema getOutputSchema() {
    try {
      final Class<SpecificRecord> outputSchemaClass = (Class<SpecificRecord>) Class
          .forName(this.viewOutputSchemaClassName);
      final SpecificRecord schemaObject = outputSchemaClass.getDeclaredConstructor().newInstance();
      return schemaObject.getSchema();
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to get schema from ViewOutputSchemaClass: " + this.viewOutputSchemaClassName, e);
    }
  }

  public static class PinotTableSpec {

    // Cluster configs
    private String controllerHost;
    private String controllerPort;

    // Table configs;
    private String tableName;
    private String loadMode;

    private List<String> dimensionColumns;
    private List<String> metricColumns;
    @Optional
    private List<String> dateTimeColumns = Collections.EMPTY_LIST;

    // Table index configs
    private List<String> invertedIndexColumns;
    private List<String> noDictionaryColumns;
    private List<String> bloomFilterColumns;
    private List<String> rangeIndexColumns;
    private Map<String, Object> columnsMaxLength;

    // Stream configs
    private Map<String, Object> streamConfigs;

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

    public PinotTableSpec() {

    }

    public String getControllerHost() {
      return this.controllerHost;
    }

    public void setControllerHost(String controllerHost) {
      this.controllerHost = controllerHost;
    }

    public String getControllerPort() {
      return this.controllerPort;
    }

    public void setControllerPort(String controllerPort) {
      this.controllerPort = controllerPort;
    }

    public String getTimeColumn() {
      return this.timeColumn;
    }

    public void setTimeColumn(String timeColumn) {
      this.timeColumn = timeColumn;
    }

    public TimeUnit getTimeUnit() {
      return this.timeUnit;
    }

    public void setTimeUnit(TimeUnit timeUnit) {
      this.timeUnit = timeUnit;
    }

    public List<String> getDimensionColumns() {
      return this.dimensionColumns;
    }

    public void setDimensionColumns(List<String> dimensionColumns) {
      this.dimensionColumns = dimensionColumns;
    }

    public List<String> getMetricColumns() {
      return this.metricColumns;
    }

    public void setMetricColumns(List<String> metricColumns) {
      this.metricColumns = metricColumns;
    }

    public List<String> getDateTimeColumns() {
      return this.dateTimeColumns;
    }

    public void setDateTimeColumns(List<String> dateTimeColumns) {
      this.dateTimeColumns = dateTimeColumns;
    }

    public Map<String, Object> getColumnsMaxLength() {
      return columnsMaxLength;
    }

    public void setColumnsMaxLength(Map<String, Object> columnsMaxLength) {
      this.columnsMaxLength = columnsMaxLength;
    }

    public List<String> getInvertedIndexColumns() {
      return invertedIndexColumns;
    }

    public void setInvertedIndexColumns(List<String> invertedIndexColumns) {
      this.invertedIndexColumns = invertedIndexColumns;
    }

    public String getTableName() {
      return this.tableName;
    }

    public void setTableName(String tableName) {
      this.tableName = tableName;
    }

    public Map<String, Object> getStreamConfigs() {
      return this.streamConfigs;
    }

    public void setStreamConfigs(Map<String, Object> streamConfigs) {
      this.streamConfigs = streamConfigs;
    }

    public String getLoadMode() {
      return this.loadMode;
    }

    public void setLoadMode(String loadMode) {
      this.loadMode = loadMode;
    }

    public int getNumReplicas() {
      return this.numReplicas;
    }

    public void setNumReplicas(int numReplicas) {
      this.numReplicas = numReplicas;
    }

    public String getRetentionTimeValue() {
      return this.retentionTimeValue;
    }

    public void setRetentionTimeValue(String retentionTimeValue) {
      this.retentionTimeValue = retentionTimeValue;
    }

    public String getRetentionTimeUnit() {
      return this.retentionTimeUnit;
    }

    public void setRetentionTimeUnit(String retentionTimeUnit) {
      this.retentionTimeUnit = retentionTimeUnit;
    }

    public String getBrokerTenant() {
      return this.brokerTenant;
    }

    public void setBrokerTenant(String brokerTenant) {
      this.brokerTenant = brokerTenant;
    }

    public String getServerTenant() {
      return this.serverTenant;
    }

    public void setServerTenant(String serverTenant) {
      this.serverTenant = serverTenant;
    }

    public String getSegmentAssignmentStrategy() {
      return this.segmentAssignmentStrategy;
    }

    public void setSegmentAssignmentStrategy(String segmentAssignmentStrategy) {
      this.segmentAssignmentStrategy = segmentAssignmentStrategy;
    }

    public List<String> getNoDictionaryColumns() {
      return noDictionaryColumns;
    }

    public void setNoDictionaryColumns(List<String> noDictionaryColumns) {
      this.noDictionaryColumns = noDictionaryColumns;
    }

    public List<String> getBloomFilterColumns() {
      return bloomFilterColumns;
    }

    public void setBloomFilterColumns(List<String> bloomFilterColumns) {
      this.bloomFilterColumns = bloomFilterColumns;
    }

    public List<String> getRangeIndexColumns() {
      return rangeIndexColumns;
    }

    public void setRangeIndexColumns(List<String> rangeIndexColumns) {
      this.rangeIndexColumns = rangeIndexColumns;
    }
  }

  public static class KafkaSpec {

    // Cluster configs
    private String brokerAddress;

    // Topic info
    private String topicName;
    private int partitions;
    private int replicationFactor;

    public KafkaSpec() {
    }

    public String getBrokerAddress() {
      return this.brokerAddress;
    }

    public void setBrokerAddress(String brokerAddress) {
      this.brokerAddress = brokerAddress;
    }

    public String getTopicName() {
      return topicName;
    }

    public void setTopicName(String topicName) {
      this.topicName = topicName;
    }

    public int getPartitions() {
      return partitions;
    }

    public void setPartitions(int partitions) {
      this.partitions = partitions;
    }

    public int getReplicationFactor() {
      return replicationFactor;
    }

    public void setReplicationFactor(int replicationFactor) {
      this.replicationFactor = replicationFactor;
    }

    @Override
    public String toString() {
      return String.format("Topic Name: %s, Partitions: %d, ReplicationFactor: %d, BrokerAddress: %s",
          topicName, partitions, replicationFactor, brokerAddress);
    }
  }
}
