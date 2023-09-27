package org.hypertrace.core.viewcreator.pinot;

import static java.util.Objects.requireNonNull;
import static org.apache.pinot.spi.config.table.TableType.OFFLINE;
import static org.apache.pinot.spi.config.table.TableType.REALTIME;
import static org.apache.pinot.spi.data.FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.buildPinotTableConfig;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.createPinotSchema;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotOfflineTableSpec;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.getPinotRealtimeTableSpec;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.ColumnPartitionConfig;
import org.apache.pinot.spi.config.table.CompletionConfig;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.config.table.FieldConfig.EncodingType;
import org.apache.pinot.spi.config.table.FieldConfig.IndexType;
import org.apache.pinot.spi.config.table.IndexingConfig;
import org.apache.pinot.spi.config.table.RoutingConfig;
import org.apache.pinot.spi.config.table.SegmentPartitionConfig;
import org.apache.pinot.spi.config.table.StarTreeIndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TenantConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.ingestion.FilterConfig;
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
                    requireNonNull(
                            this.getClass()
                                .getClassLoader()
                                .getResource("sample-view-generation-spec.conf"))
                        .getPath())));
    final PinotTableSpec pinotTableSpec = getPinotRealtimeTableSpec(viewCreationSpec);

    final Schema pinotSchema = createPinotSchema(viewCreationSpec, pinotTableSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchema);
    assertEquals(viewCreationSpec.getViewName(), pinotSchema.getSchemaName());
    // creation_time_millis not included in dimension columns
    assertEquals(6, pinotSchema.getDimensionNames().size());
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
    assertEquals(DataType.BOOLEAN, pinotSchema.getDimensionSpec("is_external").getDataType());
  }

  @Test
  public void testBuildRealtimeTableConfig() {
    final ViewCreationSpec viewCreationSpec =
        ViewCreationSpec.parse(
            ConfigFactory.parseFile(
                new File(
                    requireNonNull(
                            this.getClass()
                                .getClassLoader()
                                .getResource("sample-view-generation-spec.conf"))
                        .getPath())));
    final PinotTableSpec pinotTableSpec = getPinotRealtimeTableSpec(viewCreationSpec);
    final TableConfig tableConfig =
        buildPinotTableConfig(viewCreationSpec, pinotTableSpec, REALTIME);
    LOGGER.info("Pinot Table Config for View: {}", tableConfig);

    IndexingConfig indexingConfig = tableConfig.getIndexingConfig();

    assertEquals("myView1_REALTIME", tableConfig.getTableName());
    assertEquals(REALTIME, tableConfig.getTableType());
    assertEquals("MMAP", indexingConfig.getLoadMode());
    final Map<String, String> actualStreamConfigs =
        requireNonNull(indexingConfig.getStreamConfigs());
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
    TenantConfig tenantConfig = tableConfig.getTenantConfig();
    assertEquals("defaultBroker", tenantConfig.getBroker());
    assertEquals("defaultServer", tenantConfig.getServer());
    assertEquals(
        "tier-for-consuming",
        requireNonNull(tenantConfig.getTagOverrideConfig()).getRealtimeConsuming());
    assertEquals("tier-for-completed", tenantConfig.getTagOverrideConfig().getRealtimeCompleted());

    // Verify tier configs
    List<TierConfig> tierConfigs = requireNonNull(tableConfig.getTierConfigsList());
    assertEquals(1, tierConfigs.size());
    TierConfig tierConfig = tierConfigs.get(0);
    assertEquals("hot-data-tier", tierConfig.getName());
    assertEquals("time", tierConfig.getSegmentSelectorType());
    assertEquals("5d", tierConfig.getSegmentAge());
    assertEquals("pinot_server", tierConfig.getStorageType());
    assertEquals("tier-for-hot-data", tierConfig.getServerTag());

    // Verify indexing related configs
    assertEquals("tenant_id", requireNonNull(indexingConfig.getSortedColumn()).get(0));
    assertTrue(
        indexingConfig
            .getRangeIndexColumns()
            .containsAll(List.of("creation_time_millis", "start_time_millis")));
    assertEquals(List.of("properties__VALUES"), indexingConfig.getNoDictionaryColumns());
    assertEquals(List.of("id_sha"), indexingConfig.getBloomFilterColumns());
    assertFalse(indexingConfig.isAggregateMetrics());

    // star-tree index configs
    StarTreeIndexConfig starTreeIndexConfig =
        requireNonNull(indexingConfig.getStarTreeIndexConfigs()).get(0);
    assertEquals(
        List.of("tenant_id", "start_time_millis"), starTreeIndexConfig.getDimensionsSplitOrder());
    assertEquals(List.of(), starTreeIndexConfig.getSkipStarNodeCreationForDimensions());
    assertEquals(
        List.of("COUNT__name", "SUM__time_taken_millis"),
        starTreeIndexConfig.getFunctionColumnPairs());
    assertEquals(100, starTreeIndexConfig.getMaxLeafRecords());

    // segment partition configs
    SegmentPartitionConfig segmentPartitionConfig = indexingConfig.getSegmentPartitionConfig();
    Map<String, ColumnPartitionConfig> columnPartitionMap =
        requireNonNull(segmentPartitionConfig).getColumnPartitionMap();
    assertEquals(1, columnPartitionMap.size());
    assertEquals("HashCode", columnPartitionMap.get("tenant_id").getFunctionName());
    assertEquals(4, columnPartitionMap.get("tenant_id").getNumPartitions());

    // routing config
    RoutingConfig routingConfig = requireNonNull(tableConfig.getRoutingConfig());
    assertEquals(List.of("time", "partition"), routingConfig.getSegmentPrunerTypes());
    assertEquals("replicaGroup", routingConfig.getInstanceSelectorType());

    // Verify segment configs
    assertEquals(2, tableConfig.getValidationConfig().getReplicationNumber());
    assertEquals(1, tableConfig.getValidationConfig().getReplicasPerPartitionNumber());
    assertEquals("3", tableConfig.getValidationConfig().getRetentionTimeValue());
    assertEquals("DAYS", tableConfig.getValidationConfig().getRetentionTimeUnit());
    assertEquals(
        "BalanceNumSegmentAssignmentStrategy",
        tableConfig.getValidationConfig().getSegmentAssignmentStrategy());
    assertEquals("http", tableConfig.getValidationConfig().getPeerSegmentDownloadScheme());

    // Verify task configs
    final Map<String, String> taskConfig =
        requireNonNull(tableConfig.getTaskConfig())
            .getConfigsForTaskType("RealtimeToOfflineSegmentsTask");
    assertEquals(1, tableConfig.getTaskConfig().getTaskTypeConfigsMap().size());
    assertEquals("6h", taskConfig.get("bucketTimePeriod"));
    assertEquals("12h", taskConfig.get("bufferTimePeriod"));

    assertEquals("tenant_id", indexingConfig.getSortedColumn().get(0));
    assertEquals("creation_time_millis", tableConfig.getValidationConfig().getTimeColumnName());
    // TODO: This is deprecated
    assertEquals(TimeUnit.MILLISECONDS, tableConfig.getValidationConfig().getTimeType());

    // verify transformation configs
    TransformConfig transformConfig =
        requireNonNull(requireNonNull(tableConfig.getIngestionConfig()).getTransformConfigs())
            .get(0);
    assertEquals("bucket_start_time_millis", transformConfig.getColumnName());
    assertEquals("round(start_time_millis, 3600000)", transformConfig.getTransformFunction());

    // verify filter config
    FilterConfig filterConfig =
        requireNonNull(requireNonNull(tableConfig.getIngestionConfig()).getFilterConfig());
    assertEquals("strcmp(customer_id, 'abcd-1234') != 0", filterConfig.getFilterFunction());

    // verify field configs
    List<FieldConfig> fieldConfigs = requireNonNull(tableConfig.getFieldConfigList());
    assertEquals(1, fieldConfigs.size());
    FieldConfig fieldConfig = fieldConfigs.get(0);
    assertEquals("response_body", fieldConfig.getName());
    assertEquals(EncodingType.RAW, fieldConfig.getEncodingType());
    assertEquals(IndexType.TEXT, fieldConfig.getIndexType());
    assertEquals(fieldConfig.getProperties().get("fstType"), "lucene");
    assertEquals(fieldConfig.getProperties().get("skipExistingSegments"), "true");

    // verify completion configs
    CompletionConfig completionConfig = tableConfig.getValidationConfig().getCompletionConfig();
    assertNotNull(completionConfig);
    assertEquals(completionConfig.getCompletionMode(), "DOWNLOAD");
  }

  @Test
  public void testBuildOfflineTableConfig() {
    final ViewCreationSpec viewCreationSpec =
        ViewCreationSpec.parse(
            ConfigFactory.parseFile(
                new File(
                    requireNonNull(
                            this.getClass()
                                .getClassLoader()
                                .getResource("sample-view-generation-spec.conf"))
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
    assertEquals(2, tableConfig.getValidationConfig().getReplicationNumber());
    assertEquals("90", tableConfig.getValidationConfig().getRetentionTimeValue());
    assertEquals("DAYS", tableConfig.getValidationConfig().getRetentionTimeUnit());
    assertEquals(
        "BalanceNumSegmentAssignmentStrategy",
        tableConfig.getValidationConfig().getSegmentAssignmentStrategy());

    assertEquals("creation_time_millis", tableConfig.getValidationConfig().getTimeColumnName());
    // TODO: This is deprecated
    assertEquals(TimeUnit.MILLISECONDS, tableConfig.getValidationConfig().getTimeType());
  }
}
