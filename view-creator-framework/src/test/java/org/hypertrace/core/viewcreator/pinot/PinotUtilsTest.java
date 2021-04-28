package org.hypertrace.core.viewcreator.pinot;

import static org.apache.pinot.spi.config.table.TableType.REALTIME;
import static org.apache.pinot.spi.data.FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;
import static org.hypertrace.core.viewcreator.pinot.PinotUtils.*;
import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
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
  public void testCreatePinotSchemaForView() {
    final ViewCreationSpec viewCreationSpec =
        ViewCreationSpec.parse(
            ConfigFactory.parseFile(
                new File(
                    this.getClass()
                        .getClassLoader()
                        .getResource("sample-view-generation-spec.conf")
                        .getPath())));
    final PinotTableSpec pinotTableSpec = getPinotRealtimeTableSpec(viewCreationSpec);

    final Schema pinotSchemaForView = createPinotSchemaForView(viewCreationSpec, pinotTableSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchemaForView);
    assertEquals(viewCreationSpec.getViewName(), pinotSchemaForView.getSchemaName());
    // creation_time_millis not included in dimension columns
    assertEquals(5, pinotSchemaForView.getDimensionNames().size());
    assertEquals(1, pinotSchemaForView.getMetricFieldSpecs().size());
    assertEquals(DataType.STRING, pinotSchemaForView.getDimensionSpec("name").getDataType());
    assertEquals(DataType.BYTES, pinotSchemaForView.getDimensionSpec("id_sha").getDataType());
    assertEquals(64, pinotSchemaForView.getDimensionSpec("id_sha").getMaxLength());
    assertFalse(pinotSchemaForView.getDimensionSpec("friends").isSingleValueField());
    assertFalse(pinotSchemaForView.getDimensionSpec("properties__KEYS").isSingleValueField());
    assertEquals(
        DataType.STRING, pinotSchemaForView.getDimensionSpec("properties__KEYS").getDataType());
    assertEquals("", pinotSchemaForView.getDimensionSpec("properties__KEYS").getDefaultNullValue());
    assertFalse(pinotSchemaForView.getDimensionSpec("properties__VALUES").isSingleValueField());
    assertEquals(
        DataType.STRING, pinotSchemaForView.getDimensionSpec("properties__VALUES").getDataType());
    assertEquals("", pinotSchemaForView.getDimensionSpec("properties__KEYS").getDefaultNullValue());
    assertEquals(
        DEFAULT_DIMENSION_NULL_VALUE_OF_STRING,
        pinotSchemaForView.getDimensionSpec("name").getDefaultNullValue());
    assertEquals(
        DEFAULT_DIMENSION_NULL_VALUE_OF_STRING,
        pinotSchemaForView.getDimensionSpec("friends").getDefaultNullValue());
    // metric fields are not part of dimension columns
    assertEquals("time_taken_millis", pinotSchemaForView.getMetricFieldSpecs().get(0).getName());
    assertEquals(DataType.LONG, pinotSchemaForView.getMetricFieldSpecs().get(0).getDataType());

    assertEquals(3, pinotSchemaForView.getDateTimeFieldSpecs().size());

    DateTimeFieldSpec dateTimeFieldSpec =
        pinotSchemaForView.getDateTimeSpec("creation_time_millis");
    assertEquals("creation_time_millis", dateTimeFieldSpec.getName());
    assertEquals(
        TimeUnit.MILLISECONDS,
        new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit());
    assertEquals(DataType.LONG, dateTimeFieldSpec.getDataType());
    assertEquals(-1L, dateTimeFieldSpec.getDefaultNullValue());

    dateTimeFieldSpec = pinotSchemaForView.getDateTimeSpec("start_time_millis");
    assertEquals("start_time_millis", dateTimeFieldSpec.getName());
    assertEquals(
        TimeUnit.MILLISECONDS,
        new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit());
    assertEquals(DataType.LONG, dateTimeFieldSpec.getDataType());
    assertEquals(0L, dateTimeFieldSpec.getDefaultNullValue());
  }

  @Test
  public void testCreatePinotTableForView() {
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
    assertEquals("kafka", tableConfig.getIndexingConfig().getStreamConfigs().get("streamType"));
    assertEquals(
        "LowLevel",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.consumer.type"));
    assertEquals(
        "test-view-events",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.topic.name"));
    assertEquals(
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        tableConfig
            .getIndexingConfig()
            .getStreamConfigs()
            .get("stream.kafka.consumer.factory.class.name"));
    assertEquals(
        "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.decoder.class.name"));
    assertEquals(
        "localhost:2181",
        tableConfig
            .getIndexingConfig()
            .getStreamConfigs()
            .get("stream.kafka.hlc.zk.connect.string"));
    assertEquals(
        "localhost:2181",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.zk.broker.url"));
    assertEquals(
        "localhost:9092",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.broker.list"));
    assertEquals(
        "http://localhost:8081",
        tableConfig
            .getIndexingConfig()
            .getStreamConfigs()
            .get("stream.kafka.decoder.prop.schema.registry.rest.url"));
    assertEquals(
        "3600000",
        tableConfig
            .getIndexingConfig()
            .getStreamConfigs()
            .get("realtime.segment.flush.threshold.time"));
    assertEquals(
        "500000",
        tableConfig
            .getIndexingConfig()
            .getStreamConfigs()
            .get("realtime.segment.flush.threshold.size"));
    assertEquals(
        "largest",
        tableConfig
            .getIndexingConfig()
            .getStreamConfigs()
            .get("stream.kafka.consumer.prop.auto.offset.reset"));

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
    assertEquals("3", tableConfig.getValidationConfig().getRetentionTimeValue());
    assertEquals("DAYS", tableConfig.getValidationConfig().getRetentionTimeUnit());
    assertEquals(
        "BalanceNumSegmentAssignmentStrategy",
        tableConfig.getValidationConfig().getSegmentAssignmentStrategy());

    // TODO: This is deprecated
    assertEquals("creation_time_millis", tableConfig.getValidationConfig().getTimeColumnName());
    // TODO: This is deprecated
    assertEquals(TimeUnit.MILLISECONDS, tableConfig.getValidationConfig().getTimeType());
  }
}
