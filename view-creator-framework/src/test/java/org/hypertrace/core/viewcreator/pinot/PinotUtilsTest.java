package org.hypertrace.core.viewcreator.pinot;

import static org.apache.pinot.spi.data.FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.Schema;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotUtilsTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotUtilsTest.class);

  @Test
  public void testCreatePinotSchemaForView() {
    ViewCreationSpec viewCreationSpec = ViewCreationSpec.parse(ConfigFactory.parseFile(
        new File(this.getClass().getClassLoader()
            .getResource("sample-view-generation-spec.conf").getPath())));
    final Schema pinotSchemaForView = PinotUtils.createPinotSchemaForView(viewCreationSpec);
    LOGGER.info("Convert Pinot Schema from View: {}", pinotSchemaForView);
    Assertions.assertEquals(viewCreationSpec.getViewName(), pinotSchemaForView.getSchemaName());
    // creation_time_millis not included in dimension columns
    Assertions.assertEquals(5, pinotSchemaForView.getDimensionNames().size());
    Assertions.assertEquals(1, pinotSchemaForView.getMetricFieldSpecs().size());
    Assertions.assertEquals(DataType.STRING, pinotSchemaForView.getDimensionSpec("name").getDataType());
    Assertions.assertEquals(DataType.BYTES, pinotSchemaForView.getDimensionSpec("id_sha").getDataType());
    Assertions.assertEquals(64, pinotSchemaForView.getDimensionSpec("id_sha").getMaxLength());
    Assertions.assertFalse(pinotSchemaForView.getDimensionSpec("friends").isSingleValueField());
    Assertions.assertFalse(pinotSchemaForView.getDimensionSpec("properties__KEYS").isSingleValueField());
    Assertions.assertEquals(DataType.STRING, pinotSchemaForView.getDimensionSpec("properties__KEYS").getDataType());
    Assertions.assertEquals("",
        pinotSchemaForView.getDimensionSpec("properties__KEYS").getDefaultNullValue());
    Assertions.assertFalse(pinotSchemaForView.getDimensionSpec("properties__VALUES").isSingleValueField());
    Assertions.assertEquals(DataType.STRING,
        pinotSchemaForView.getDimensionSpec("properties__VALUES").getDataType());
    Assertions.assertEquals("",
        pinotSchemaForView.getDimensionSpec("properties__KEYS").getDefaultNullValue());
    Assertions.assertEquals(DEFAULT_DIMENSION_NULL_VALUE_OF_STRING, pinotSchemaForView.getDimensionSpec("name")
                                            .getDefaultNullValue());
    Assertions.assertEquals(DEFAULT_DIMENSION_NULL_VALUE_OF_STRING, pinotSchemaForView.getDimensionSpec("friends")
                                            .getDefaultNullValue());
    // metric fields are not part of dimension columns
    Assertions.assertEquals("time_taken_millis", pinotSchemaForView.getMetricFieldSpecs().get(0).getName());
    Assertions.assertEquals(DataType.LONG, pinotSchemaForView.getMetricFieldSpecs().get(0).getDataType());

    Assertions.assertEquals("creation_time_millis", pinotSchemaForView.getTimeFieldSpec().getName());
    Assertions.assertEquals(TimeUnit.MILLISECONDS,
        pinotSchemaForView.getTimeFieldSpec().getIncomingGranularitySpec().getTimeType());
    Assertions.assertEquals(DataType.LONG, pinotSchemaForView.getTimeFieldSpec().getDataType());
    Assertions.assertEquals(-1L, pinotSchemaForView.getTimeFieldSpec().getDefaultNullValue());
  }

  @Test
  public void testCreatePinotTableForView() {
    ViewCreationSpec viewCreationSpec = ViewCreationSpec.parse(ConfigFactory.parseFile(
        new File(this.getClass().getClassLoader()
            .getResource("sample-view-generation-spec.conf").getPath())));

    final TableConfig tableConfig = PinotUtils.createPinotTableConfig(viewCreationSpec);

    LOGGER.info("Pinot Table Config for View: {}", tableConfig);
    Assertions.assertEquals("myView1_REALTIME", tableConfig.getTableName());
    Assertions.assertEquals(TableType.REALTIME, tableConfig.getTableType());
    Assertions.assertEquals("MMAP", tableConfig.getIndexingConfig().getLoadMode());
    Assertions.assertEquals("kafka", tableConfig.getIndexingConfig().getStreamConfigs().get("streamType"));
    Assertions.assertEquals("LowLevel",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.consumer.type"));
    Assertions.assertEquals("test-view-events",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.topic.name"));
    Assertions.assertEquals("org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.consumer.factory.class.name"));
    Assertions.assertEquals("org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.decoder.class.name"));
    Assertions.assertEquals("localhost:2181",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.hlc.zk.connect.string"));
    Assertions.assertEquals("localhost:2181",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.zk.broker.url"));
    Assertions.assertEquals("localhost:9092",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.broker.list"));
    Assertions.assertEquals("http://localhost:8081",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.decoder.prop.schema.registry.rest.url"));
    Assertions.assertEquals("3600000",
        tableConfig.getIndexingConfig().getStreamConfigs().get("realtime.segment.flush.threshold.time"));
    Assertions.assertEquals("500000",
        tableConfig.getIndexingConfig().getStreamConfigs().get("realtime.segment.flush.threshold.size"));
    Assertions.assertEquals("largest",
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.consumer.prop.auto.offset.reset"));

    // Verify tenant configs
    Assertions.assertEquals("defaultBroker", tableConfig.getTenantConfig().getBroker());
    Assertions.assertEquals("defaultServer", tableConfig.getTenantConfig().getServer());

    // Verify indexing related configs
    Assertions.assertEquals(List.of("creation_time_millis"),
        tableConfig.getIndexingConfig().getRangeIndexColumns());
    Assertions.assertEquals(List.of("properties__VALUES"),
        tableConfig.getIndexingConfig().getNoDictionaryColumns());
    Assertions.assertEquals(List.of("id_sha"), tableConfig.getIndexingConfig().getBloomFilterColumns());

    // Verify segment configs
    Assertions.assertEquals(1, tableConfig.getValidationConfig().getReplicationNumber());
    Assertions.assertEquals("3", tableConfig.getValidationConfig().getRetentionTimeValue());
    Assertions.assertEquals("DAYS", tableConfig.getValidationConfig().getRetentionTimeUnit());
    Assertions.assertEquals("BalanceNumSegmentAssignmentStrategy",
        tableConfig.getValidationConfig().getSegmentAssignmentStrategy());

    // TODO: This is deprecated
    Assertions.assertEquals("creation_time_millis", tableConfig.getValidationConfig().getTimeColumnName());
    // TODO: This is deprecated
    Assertions.assertEquals(TimeUnit.MILLISECONDS, tableConfig.getValidationConfig().getTimeType());
  }
}
