package org.hypertrace.core.viewcreator.pinot;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.utils.CommonConstants.Helix.TableType;
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

    // metric fields are not part of dimension columns
    Assertions.assertEquals("time_taken_millis", pinotSchemaForView.getMetricFieldSpecs().get(0).getName());
    Assertions.assertEquals(DataType.LONG, pinotSchemaForView.getMetricFieldSpecs().get(0).getDataType());

    Assertions.assertEquals("creation_time_millis", pinotSchemaForView.getTimeColumnName());
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

    final TableConfig tableConfig = PinotUtils.createPinotTable(viewCreationSpec);

    LOGGER.info("Pinot Table Config for View: {}", tableConfig);
    Assertions.assertEquals(tableConfig.getTableName(), "myView1_REALTIME");
    Assertions.assertEquals(tableConfig.getTableType(), TableType.REALTIME);
    Assertions.assertEquals(tableConfig.getIndexingConfig().getLoadMode(), "MMAP");
    Assertions.assertEquals(tableConfig.getIndexingConfig().getStreamConfigs().get("streamType"),
        "kafka");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.consumer.type"),
        "LowLevel");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.topic.name"),
        "test-view-events");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.consumer.factory.class.name"),
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.decoder.class.name"),
        "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder");
    Assertions.assertEquals(tableConfig.getIndexingConfig().getStreamConfigs()
        .get("stream.kafka.hlc.zk.connect.string"), "localhost:2181");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.zk.broker.url"),
        "localhost:2181");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.broker.list"),
        "localhost:9092");
    Assertions.assertEquals(
        tableConfig.getIndexingConfig().getStreamConfigs().get("stream.kafka.decoder.prop.schema.registry.rest.url"),
        "http://localhost:8081");
    Assertions.assertEquals(tableConfig.getIndexingConfig().getStreamConfigs()
        .get("realtime.segment.flush.threshold.time"), "3600000");
    Assertions.assertEquals(tableConfig.getIndexingConfig().getStreamConfigs()
        .get("realtime.segment.flush.threshold.size"), "500000");
    Assertions.assertEquals(tableConfig.getIndexingConfig().getStreamConfigs()
        .get("stream.kafka.consumer.prop.auto.offset.reset"), "largest");

    Assertions.assertEquals(tableConfig.getTenantConfig().getBroker(), "defaultBroker");
    Assertions.assertEquals(tableConfig.getTenantConfig().getServer(), "defaultServer");
    Assertions.assertEquals(tableConfig.getValidationConfig().getReplicationNumber(), 1);
    Assertions.assertEquals(tableConfig.getValidationConfig().getRetentionTimeValue(), "3");
    Assertions.assertEquals(tableConfig.getValidationConfig().getRetentionTimeUnit(), "DAYS");
    // TODO: This is deprecated
    Assertions.assertEquals(tableConfig.getValidationConfig().getTimeColumnName(), "creation_time_millis");
    // TODO: This is deprecated
    Assertions.assertEquals(tableConfig.getValidationConfig().getTimeType(), TimeUnit.MILLISECONDS);
    Assertions.assertEquals(tableConfig.getValidationConfig().getSegmentAssignmentStrategy(),
        "BalanceNumSegmentAssignmentStrategy");
  }
}
