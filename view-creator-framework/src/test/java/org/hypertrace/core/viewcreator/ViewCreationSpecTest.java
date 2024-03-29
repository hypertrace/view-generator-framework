package org.hypertrace.core.viewcreator;

import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.COMPLETION_CONFIG_COMPLETION_MODE;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_FILTER_FUNCTION;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TRANSFORM_COLUMN_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.viewcreator.pinot.PinotTableSpec;
import org.hypertrace.core.viewcreator.pinot.PinotUtils;
import org.hypertrace.core.viewcreator.test.api.TestView;
import org.junit.jupiter.api.Test;

public class ViewCreationSpecTest {

  @Test
  public void testViewGeneratorSpecParser() {
    ViewCreationSpec viewCreationSpec =
        createViewCreationSpecUsingConfig("sample-view-generation-spec.conf");

    // Test ViewCreationSpec
    assertEquals(viewCreationSpec.getViewName(), "myView");
    assertEquals(viewCreationSpec.getOutputSchema(), TestView.getClassSchema());

    // Test PinotTableSpec
    final PinotTableSpec pinotTableSpec = PinotUtils.getPinotRealtimeTableSpec(viewCreationSpec);
    assertEquals(pinotTableSpec.getControllerHost(), "localhost");
    assertEquals(pinotTableSpec.getControllerPort(), "9000");
    assertEquals(pinotTableSpec.getTimeColumn(), "creation_time_millis");
    assertEquals(pinotTableSpec.getSortedColumn(), "tenant_id");
    assertEquals(pinotTableSpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    assertEquals(
        pinotTableSpec.getDimensionColumns(),
        List.of(
            "name",
            "creation_time_millis",
            "id_sha",
            "friends",
            "properties__KEYS",
            "properties__VALUES"));
    assertEquals(pinotTableSpec.getMetricColumns(), List.of("time_taken_millis"));
    assertEquals(pinotTableSpec.getColumnsMaxLength(), Map.of("id_sha", 64));
    assertEquals(
        pinotTableSpec.getInvertedIndexColumns(),
        List.of("friends", "properties__KEYS", "properties__VALUES"));

    assertEquals(pinotTableSpec.getTableName(), "myView1");
    assertEquals(pinotTableSpec.getLoadMode(), "MMAP");
    assertEquals(pinotTableSpec.getNumReplicas(), 2);
    assertEquals(pinotTableSpec.getReplicasPerPartition(), "1");
    assertEquals(pinotTableSpec.getRetentionTimeUnit(), "DAYS");
    assertEquals(pinotTableSpec.getRetentionTimeValue(), "3");
    assertEquals(pinotTableSpec.getBrokerTenant(), "defaultBroker");
    assertEquals(pinotTableSpec.getServerTenant(), "defaultServer");
    assertEquals(
        pinotTableSpec.getSegmentAssignmentStrategy(), "BalanceNumSegmentAssignmentStrategy");

    final Map<String, Object> streamConfigs = pinotTableSpec.getStreamConfigs();
    assertEquals(streamConfigs.get("streamType"), "kafka");
    assertEquals(streamConfigs.get("stream.kafka.consumer.type"), "LowLevel");
    assertEquals(streamConfigs.get("stream.kafka.topic.name"), "test-view-events");
    assertEquals(
        streamConfigs.get("stream.kafka.consumer.factory.class.name"),
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
    assertEquals(
        streamConfigs.get("stream.kafka.decoder.class.name"),
        "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder");
    assertEquals(
        streamConfigs.get("stream.kafka.decoder.prop.schema.registry.rest.url"),
        "http://localhost:8081");
    assertEquals(streamConfigs.get("stream.kafka.hlc.zk.connect.string"), "localhost:2181");
    assertEquals(streamConfigs.get("stream.kafka.zk.broker.url"), "localhost:2181");
    assertEquals(streamConfigs.get("stream.kafka.broker.list"), "localhost:9092");
    assertEquals(streamConfigs.get("realtime.segment.flush.threshold.size"), "500000");
    assertEquals(streamConfigs.get("realtime.segment.flush.threshold.time"), "3600000");
    assertEquals(streamConfigs.get("stream.kafka.consumer.prop.auto.offset.reset"), "largest");

    Config transformConfig = pinotTableSpec.getTransformConfigs().get(0);
    assertEquals(
        transformConfig.getString(PINOT_TRANSFORM_COLUMN_NAME), "bucket_start_time_millis");

    Config filterConfig = pinotTableSpec.getFilterConfig();
    assertEquals(
        filterConfig.getString(PINOT_FILTER_FUNCTION), "strcmp(customer_id, 'abcd-1234') != 0");

    Config fieldConfig = pinotTableSpec.getFieldConfigs().get(0);
    assertEquals(fieldConfig.getString("name"), "response_body");
    assertEquals(fieldConfig.getBoolean("properties.skipExistingSegments"), true);

    Config completionConfig = pinotTableSpec.getCompletionConfig();
    assertEquals(completionConfig.getString(COMPLETION_CONFIG_COMPLETION_MODE), "DOWNLOAD");
  }

  @Test
  public void testAvroDecoderSchemaIsAutoSetInTableCreationRequest() {
    ViewCreationSpec viewCreationSpec =
        createViewCreationSpecUsingConfig("sample-view-generation-spec-without-schema.conf");
    final Map<String, Object> streamConfigs =
        PinotUtils.getPinotRealtimeTableSpec(viewCreationSpec).getStreamConfigs();
    assertEquals(
        streamConfigs.get("stream.kafka.decoder.prop.schema"),
        TestView.getClassSchema().toString());
  }

  @Test
  public void testMissingViewOutputSchemaClassConfig() {
    assertThrows(
        ConfigException.Missing.class,
        () -> createViewCreationSpecUsingConfig("missing_view_output_schema_class.conf"));
  }

  @Test
  public void testMissingViewName() {
    assertThrows(
        ConfigException.Missing.class,
        () -> createViewCreationSpecUsingConfig("missing_view_name.conf"));
  }

  private ViewCreationSpec createViewCreationSpecUsingConfig(String resourcePath) {
    File configFile =
        new File(this.getClass().getClassLoader().getResource(resourcePath).getPath());
    Config configs = ConfigFactory.parseFile(configFile);
    return ViewCreationSpec.parse(configs);
  }
}
