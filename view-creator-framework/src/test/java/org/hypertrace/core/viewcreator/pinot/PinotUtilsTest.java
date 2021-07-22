package org.hypertrace.core.viewcreator.pinot;

import static org.apache.pinot.spi.config.table.TableType.OFFLINE;
import static org.apache.pinot.spi.config.table.TableType.REALTIME;
import static org.apache.pinot.spi.data.FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.buildPinotTableConfig;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.createPinotSchema;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotOfflineTableSpec;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotRealtimeTableSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotUtilsTest.class);

  @Test
  public void testCreatePinotSchema() {
    final ViewCreationSpec viewCreationSpec =
        ViewCreationSpec.parse(
            ConfigFactory.parseFile(
                new File(
                    this.getClass()
                        .getClassLoader()
                        .getResource("sample-view-generation-spec.conf")
                        .getPath())));
    final PinotTableSpec pinotTableSpec = getPinotRealtimeTableSpec(viewCreationSpec);

    final Schema pinotSchema = createPinotSchema(viewCreationSpec, pinotTableSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchema);
    assertEquals(viewCreationSpec.getViewName(), pinotSchema.getSchemaName());
    // creation_time_millis not included in dimension columns
    assertEquals(5, pinotSchema.getDimensionNames().size());
    assertEquals(1, pinotSchema.getMetricFieldSpecs().size());
    assertEquals(DataType.STRING, pinotSchema.getDimensionSpec("name").getDataType());
    assertEquals(DataType.BYTES, pinotSchema.getDimensionSpec("id_sha").getDataType());
    assertEquals(64, pinotSchema.getDimensionSpec("id_sha").getMaxLength());
    assertFalse(pinotSchema.getDimensionSpec("friends").isSingleValueField());
    assertFalse(pinotSchema.getDimensionSpec("properties__KEYS").isSingleValueField());
    assertEquals(DataType.STRING, pinotSchema.getDimensionSpec("properties__KEYS").getDataType());
    assertEquals("", pinotSchema.getDimensionSpec("properties__KEYS").getDefaultNullValue());
    assertFalse(pinotSchema.getDimensionSpec("properties__VALUES").isSingleValueField());
    assertEquals(DataType.STRING, pinotSchema.getDimensionSpec("properties__VALUES").getDataType());
    assertEquals("", pinotSchema.getDimensionSpec("properties__KEYS").getDefaultNullValue());
    assertEquals(
        DEFAULT_DIMENSION_NULL_VALUE_OF_STRING,
        pinotSchema.getDimensionSpec("name").getDefaultNullValue());
    assertEquals(
        DEFAULT_DIMENSION_NULL_VALUE_OF_STRING,
        pinotSchema.getDimensionSpec("friends").getDefaultNullValue());
    // metric fields are not part of dimension columns
    assertEquals("time_taken_millis", pinotSchema.getMetricFieldSpecs().get(0).getName());
    assertEquals(DataType.LONG, pinotSchema.getMetricFieldSpecs().get(0).getDataType());

    assertEquals(3, pinotSchema.getDateTimeFieldSpecs().size());

    DateTimeFieldSpec dateTimeFieldSpec = pinotSchema.getDateTimeSpec("creation_time_millis");
    assertEquals("creation_time_millis", dateTimeFieldSpec.getName());
    assertEquals(
        TimeUnit.MILLISECONDS,
        new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit());
    assertEquals(DataType.LONG, dateTimeFieldSpec.getDataType());
    assertEquals(-1L, dateTimeFieldSpec.getDefaultNullValue());

    dateTimeFieldSpec = pinotSchema.getDateTimeSpec("start_time_millis");
    assertEquals("start_time_millis", dateTimeFieldSpec.getName());
    assertEquals(
        TimeUnit.MILLISECONDS,
        new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit());
    assertEquals(DataType.LONG, dateTimeFieldSpec.getDataType());
    assertEquals(0L, dateTimeFieldSpec.getDefaultNullValue());
  }

  @Test
  public void testBuildRealtimeTableConfig() {
    final ViewCreationSpec viewCreationSpec =
        ViewCreationSpec.parse(
            ConfigFactory.parseFile(
                new File(
                    this.getClass()
                        .getClassLoader()
                        .getResource("sample-view-generation-spec.conf")
                        .getPath())));
    final PinotTableSpec pinotTableSpec = getPinotRealtimeTableSpec(viewCreationSpec);
    final TableConfig tableConfig =
        buildPinotTableConfig(viewCreationSpec, pinotTableSpec, REALTIME);
    LOGGER.info("Pinot Table Config for View: {}", tableConfig);
    assertEquals("myView1_REALTIME", tableConfig.getTableName());
    assertEquals(REALTIME, tableConfig.getTableType());
    assertEquals("MMAP", tableConfig.getIndexingConfig().getLoadMode());
    final Map<String, String> actualStreamConfigs =
        tableConfig.getIndexingConfig().getStreamConfigs();
    assertEquals("kafka", actualStreamConfigs.get("streamType"));
    assertEquals("LowLevel", actualStreamConfigs.get("stream.kafka.consumer.type"));
    assertEquals("test-view-events", actualStreamConfigs.get("stream.kafka.topic.name"));
    assertEquals(
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        actualStreamConfigs.get("stream.kafka.consumer.factory.class.name"));
    assertEquals(
        "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
        actualStreamConfigs.get("stream.kafka.decoder.class.name"));
    assertEquals("localhost:2181", actualStreamConfigs.get("stream.kafka.hlc.zk.connect.string"));
    assertEquals("localhost:2181", actualStreamConfigs.get("stream.kafka.zk.broker.url"));
    assertEquals("localhost:9092", actualStreamConfigs.get("stream.kafka.broker.list"));
    assertEquals(
        "http://localhost:8081",
        actualStreamConfigs.get("stream.kafka.decoder.prop.schema.registry.rest.url"));
    assertEquals("3600000", actualStreamConfigs.get("realtime.segment.flush.threshold.time"));
    assertEquals("500000", actualStreamConfigs.get("realtime.segment.flush.threshold.size"));
    assertEquals(
        "largest", actualStreamConfigs.get("stream.kafka.consumer.prop.auto.offset.reset"));

    // Verify tenant configs
    assertEquals("defaultBroker", tableConfig.getTenantConfig().getBroker());
    assertEquals("defaultServer", tableConfig.getTenantConfig().getServer());

    // Verify indexing related configs
    assertTrue(
        tableConfig
            .getIndexingConfig()
            .getRangeIndexColumns()
            .containsAll(List.of("creation_time_millis", "start_time_millis")));
    assertEquals(
        List.of("properties__VALUES"), tableConfig.getIndexingConfig().getNoDictionaryColumns());
    assertEquals(List.of("id_sha"), tableConfig.getIndexingConfig().getBloomFilterColumns());
    assertEquals(false, tableConfig.getIndexingConfig().isAggregateMetrics());

    // Verify segment configs
    assertEquals(1, tableConfig.getValidationConfig().getReplicationNumber());
    assertEquals("3", tableConfig.getValidationConfig().getRetentionTimeValue());
    assertEquals("DAYS", tableConfig.getValidationConfig().getRetentionTimeUnit());
    assertEquals(
        "BalanceNumSegmentAssignmentStrategy",
        tableConfig.getValidationConfig().getSegmentAssignmentStrategy());

    // Verify task configs
    final Map<String, String> taskConfig =
        tableConfig.getTaskConfig().getConfigsForTaskType("RealtimeToOfflineSegmentsTask");
    assertEquals(1, tableConfig.getTaskConfig().getTaskTypeConfigsMap().size());
    assertEquals("6h", taskConfig.get("bucketTimePeriod"));
    assertEquals("12h", taskConfig.get("bufferTimePeriod"));

    // TODO: This is deprecated
    assertEquals("creation_time_millis", tableConfig.getValidationConfig().getTimeColumnName());
    // TODO: This is deprecated
    assertEquals(TimeUnit.MILLISECONDS, tableConfig.getValidationConfig().getTimeType());

    TransformConfig transformConfig = tableConfig.getIngestionConfig().getTransformConfigs().get(0);
    assertEquals("bucket_start_time_millis", transformConfig.getColumnName());
    assertEquals("round(start_time_millis, 3600000)", transformConfig.getTransformFunction());
  }

  @Test
  public void testBuildOfflineTableConfig() {
    final ViewCreationSpec viewCreationSpec =
        ViewCreationSpec.parse(
            ConfigFactory.parseFile(
                new File(
                    this.getClass()
                        .getClassLoader()
                        .getResource("sample-view-generation-spec.conf")
                        .getPath())));
    final PinotTableSpec pinotTableSpec = getPinotOfflineTableSpec(viewCreationSpec);
    final TableConfig tableConfig =
        buildPinotTableConfig(viewCreationSpec, pinotTableSpec, OFFLINE);
    LOGGER.info("Pinot Table Config for View: {}", tableConfig);
    assertEquals("myView1_OFFLINE", tableConfig.getTableName());
    assertEquals(OFFLINE, tableConfig.getTableType());
    assertEquals("MMAP", tableConfig.getIndexingConfig().getLoadMode());

    // Verify tenant configs
    assertEquals("defaultBroker", tableConfig.getTenantConfig().getBroker());
    assertEquals("defaultServer", tableConfig.getTenantConfig().getServer());

    // Verify indexing related configs
    assertTrue(
        tableConfig
            .getIndexingConfig()
            .getRangeIndexColumns()
            .containsAll(List.of("creation_time_millis", "start_time_millis")));
    assertEquals(
        List.of("properties__VALUES"), tableConfig.getIndexingConfig().getNoDictionaryColumns());
    assertEquals(List.of("id_sha"), tableConfig.getIndexingConfig().getBloomFilterColumns());

    // Verify segment configs
    assertEquals(1, tableConfig.getValidationConfig().getReplicationNumber());
    assertEquals("90", tableConfig.getValidationConfig().getRetentionTimeValue());
    assertEquals("DAYS", tableConfig.getValidationConfig().getRetentionTimeUnit());
    assertEquals(
        "BalanceNumSegmentAssignmentStrategy",
        tableConfig.getValidationConfig().getSegmentAssignmentStrategy());

    // TODO: This is deprecated
    assertEquals("creation_time_millis", tableConfig.getValidationConfig().getTimeColumnName());
    // TODO: This is deprecated
    assertEquals(TimeUnit.MILLISECONDS, tableConfig.getValidationConfig().getTimeType());
  }

  @Test
  public void testSendPinotTableCreationRequest() {
    String createRealtimeTableConfig =
        "{\"tableName\":\"testTable_REALTIME\",\"tableType\":\"REALTIME\",\"segmentsConfig\":{\"schemaName\":\"metricView\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"timeColumnName\":\"bucket_start_time_millis\",\"timeType\":\"MILLISECONDS\",\"retentionTimeValue\":\"3\",\"retentionTimeUnit\":\"DAYS\",\"segmentPushType\":\"APPEND\",\"replication\":\"2\",\"replicasPerPartition\":\"2\"},\"tenants\":{\"broker\":\"DefaultTenant\",\"server\":\"DefaultTenant\"},\"tableIndexConfig\":{\"streamConfigs\":{\"realtime.segment.flush.threshold.rows\":\"500000\",\"stream.kafka.hlc.zk.connect.string\":\"zookeeper:2181\",\"stream.kafka.decoder.class.name\":\"org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder\",\"streamType\":\"kafka\",\"stream.kafka.decoder.prop.schema.registry.rest.url\":\"http://schema-registry-service:8081\",\"stream.kafka.consumer.type\":\"LowLevel\",\"stream.kafka.zk.broker.url\":\"zookeeper:2181\",\"realtime.segment.flush.threshold.time\":\"4h\",\"stream.kafka.broker.list\":\"bootstrap:9092\",\"stream.kafka.consumer.factory.class.name\":\"org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory\",\"stream.kafka.consumer.prop.auto.offset.reset\":\"largest\",\"stream.kafka.topic.name\":\"span-event-view\"},\"loadMode\":\"MMAP\",\"invertedIndexColumns\":[\"customer_id\",\"api_id\",\"api_boundary_type\"],\"bloomFilterColumns\":[],\"noDictionaryColumns\":[\"call_count\"],\"rangeIndexColumns\":[\"bucket_start_time_millis\"],\"aggregateMetrics\":true,\"createInvertedIndexDuringSegmentGeneration\":false,\"nullHandlingEnabled\":false,\"enableDefaultStarTree\":false,\"enableDynamicStarTreeCreation\":false,\"autoGeneratedInvertedIndex\":false},\"metadata\":{},\"task\":{\"taskTypeConfigsMap\":{\"RealtimeToOfflineSegmentsTask\":{\"bufferTimePeriod\":\"48h\",\"bucketTimePeriod\":\"12h\"}}},\"ingestionConfig\":{\"transformConfigs\":[{\"columnName\":\"bucket_start_time_millis\",\"transformFunction\":\"round(start_time_millis, 3600000)\"}]},\"isDimTable\":false}";
    String createOfflineTableConfig =
        "{\"tableName\":\"testTable_OFFLINE\",\"tableType\":\"OFFLINE\",\"segmentsConfig\":{\"schemaName\":\"metricView\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"timeColumnName\":\"bucket_start_time_millis\",\"timeType\":\"MILLISECONDS\",\"retentionTimeValue\":\"3\",\"retentionTimeUnit\":\"DAYS\",\"segmentPushType\":\"APPEND\",\"replication\":\"2\",\"replicasPerPartition\":\"2\"},\"tenants\":{\"broker\":\"DefaultTenant\",\"server\":\"DefaultTenant\"},\"tableIndexConfig\":{\"streamConfigs\":{\"realtime.segment.flush.threshold.rows\":\"500000\",\"stream.kafka.hlc.zk.connect.string\":\"zookeeper:2181\",\"stream.kafka.decoder.class.name\":\"org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder\",\"streamType\":\"kafka\",\"stream.kafka.decoder.prop.schema.registry.rest.url\":\"http://schema-registry-service:8081\",\"stream.kafka.consumer.type\":\"LowLevel\",\"stream.kafka.zk.broker.url\":\"zookeeper:2181\",\"realtime.segment.flush.threshold.time\":\"4h\",\"stream.kafka.broker.list\":\"bootstrap:9092\",\"stream.kafka.consumer.factory.class.name\":\"org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory\",\"stream.kafka.consumer.prop.auto.offset.reset\":\"largest\",\"stream.kafka.topic.name\":\"span-event-view\"},\"loadMode\":\"MMAP\",\"invertedIndexColumns\":[\"customer_id\",\"api_id\",\"api_boundary_type\"],\"bloomFilterColumns\":[],\"noDictionaryColumns\":[\"call_count\"],\"rangeIndexColumns\":[\"bucket_start_time_millis\"],\"aggregateMetrics\":true,\"createInvertedIndexDuringSegmentGeneration\":false,\"nullHandlingEnabled\":false,\"enableDefaultStarTree\":false,\"enableDynamicStarTreeCreation\":false,\"autoGeneratedInvertedIndex\":false},\"metadata\":{},\"task\":{\"taskTypeConfigsMap\":{\"RealtimeToOfflineSegmentsTask\":{\"bufferTimePeriod\":\"48h\",\"bucketTimePeriod\":\"12h\"}}},\"ingestionConfig\":{\"transformConfigs\":[{\"columnName\":\"bucket_start_time_millis\",\"transformFunction\":\"round(start_time_millis, 3600000)\"}]},\"isDimTable\":false}";
    String updateRealtimeTableConfig =
        "{\"tableName\":\"testTable_REALTIME\",\"tableType\":\"REALTIME\",\"segmentsConfig\":{\"schemaName\":\"metricView\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"timeColumnName\":\"bucket_start_time_millis\",\"timeType\":\"MILLISECONDS\",\"retentionTimeValue\":\"3\",\"retentionTimeUnit\":\"DAYS\",\"segmentPushType\":\"APPEND\",\"replication\":\"2\",\"replicasPerPartition\":\"2\"},\"tenants\":{\"broker\":\"DefaultTenant\",\"server\":\"DefaultTenant\"},\"tableIndexConfig\":{\"streamConfigs\":{\"realtime.segment.flush.threshold.rows\":\"500000\",\"stream.kafka.hlc.zk.connect.string\":\"zookeeper:2181\",\"stream.kafka.decoder.class.name\":\"org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder\",\"streamType\":\"kafka\",\"stream.kafka.decoder.prop.schema.registry.rest.url\":\"http://schema-registry-service:8081\",\"stream.kafka.consumer.type\":\"LowLevel\",\"stream.kafka.zk.broker.url\":\"zookeeper:2181\",\"realtime.segment.flush.threshold.time\":\"4h\",\"stream.kafka.broker.list\":\"bootstrap:9092\",\"stream.kafka.consumer.factory.class.name\":\"org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory\",\"stream.kafka.consumer.prop.auto.offset.reset\":\"largest\",\"stream.kafka.topic.name\":\"span-event-view\"},\"loadMode\":\"MMAP\",\"invertedIndexColumns\":[\"customer_id\",\"api_id\",\"api_boundary_type\"],\"bloomFilterColumns\":[],\"noDictionaryColumns\":[\"call_count\"],\"rangeIndexColumns\":[\"bucket_start_time_millis\"],\"aggregateMetrics\":true,\"createInvertedIndexDuringSegmentGeneration\":false,\"nullHandlingEnabled\":false,\"enableDefaultStarTree\":false,\"enableDynamicStarTreeCreation\":false,\"autoGeneratedInvertedIndex\":false},\"metadata\":{},\"task\":{\"taskTypeConfigsMap\":{\"RealtimeToOfflineSegmentsTask\":{\"bufferTimePeriod\":\"48h\",\"bucketTimePeriod\":\"12h\"}}},\"ingestionConfig\":{\"transformConfigs\":[{\"columnName\":\"bucket_start_time_millis\",\"transformFunction\":\"round(start_time_millis, 3600000)\"}]},\"isDimTable\":false}";
    String updateOfflineTableConfig =
        "{\"tableName\":\"testTable_OFFLINE\",\"tableType\":\"OFFLINE\",\"segmentsConfig\":{\"schemaName\":\"metricView\",\"segmentAssignmentStrategy\":\"BalanceNumSegmentAssignmentStrategy\",\"timeColumnName\":\"bucket_start_time_millis\",\"timeType\":\"MILLISECONDS\",\"retentionTimeValue\":\"3\",\"retentionTimeUnit\":\"DAYS\",\"segmentPushType\":\"APPEND\",\"replication\":\"2\",\"replicasPerPartition\":\"2\"},\"tenants\":{\"broker\":\"DefaultTenant\",\"server\":\"DefaultTenant\"},\"tableIndexConfig\":{\"streamConfigs\":{\"realtime.segment.flush.threshold.rows\":\"500000\",\"stream.kafka.hlc.zk.connect.string\":\"zookeeper:2181\",\"stream.kafka.decoder.class.name\":\"org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder\",\"streamType\":\"kafka\",\"stream.kafka.decoder.prop.schema.registry.rest.url\":\"http://schema-registry-service:8081\",\"stream.kafka.consumer.type\":\"LowLevel\",\"stream.kafka.zk.broker.url\":\"zookeeper:2181\",\"realtime.segment.flush.threshold.time\":\"4h\",\"stream.kafka.broker.list\":\"bootstrap:9092\",\"stream.kafka.consumer.factory.class.name\":\"org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory\",\"stream.kafka.consumer.prop.auto.offset.reset\":\"largest\",\"stream.kafka.topic.name\":\"span-event-view\"},\"loadMode\":\"MMAP\",\"invertedIndexColumns\":[\"customer_id\",\"api_id\",\"api_boundary_type\"],\"bloomFilterColumns\":[],\"noDictionaryColumns\":[\"call_count\"],\"rangeIndexColumns\":[\"bucket_start_time_millis\"],\"aggregateMetrics\":true,\"createInvertedIndexDuringSegmentGeneration\":false,\"nullHandlingEnabled\":false,\"enableDefaultStarTree\":false,\"enableDynamicStarTreeCreation\":false,\"autoGeneratedInvertedIndex\":false},\"metadata\":{},\"task\":{\"taskTypeConfigsMap\":{\"RealtimeToOfflineSegmentsTask\":{\"bufferTimePeriod\":\"48h\",\"bucketTimePeriod\":\"12h\"}}},\"ingestionConfig\":{\"transformConfigs\":[{\"columnName\":\"bucket_start_time_millis\",\"transformFunction\":\"round(start_time_millis, 3600000)\"}]},\"isDimTable\":false}";

    assert (PinotUtils.sendPinotTableCreationRequest(
        "localhost", "9000", createRealtimeTableConfig, "testTable"));
    assert (PinotUtils.sendPinotTableCreationRequest(
        "localhost", "9000", createOfflineTableConfig, "testTable"));
    assert (PinotUtils.sendPinotTableCreationRequest(
        "localhost", "9000", updateRealtimeTableConfig, "testTable"));
    assert (PinotUtils.sendPinotTableCreationRequest(
        "localhost", "9000", updateOfflineTableConfig, "testTable"));
  }
}
