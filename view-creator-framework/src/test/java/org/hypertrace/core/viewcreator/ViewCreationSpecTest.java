package org.hypertrace.core.viewcreator;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.viewcreator.pinot.PinotUtils;
import org.hypertrace.core.viewcreator.test.api.TestView;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ViewCreationSpecTest {
  @Test
  public void testViewGeneratorSpecParser() {
    ViewCreationSpec viewCreationSpec = createViewCreationSpecUsingConfig("sample-view-generation-spec.conf");

    // Test ViewCreationSpec
    Assertions.assertEquals(viewCreationSpec.getViewName(), "myView");
    Assertions.assertEquals(viewCreationSpec.getOutputSchema(), TestView.getClassSchema());

    // Test PinotTableSpec
    final ViewCreationSpec.PinotTableSpec pinotTableSpec = PinotUtils
        .getPinotTableSpecFromViewGenerationSpec(viewCreationSpec);
    Assertions.assertEquals(pinotTableSpec.getControllerHost(), "localhost");
    Assertions.assertEquals(pinotTableSpec.getControllerPort(), "9000");
    Assertions.assertEquals(pinotTableSpec.getTimeColumn(), "creation_time_millis");
    Assertions.assertEquals(pinotTableSpec.getTimeUnit(), TimeUnit.MILLISECONDS);
    Assertions.assertEquals(pinotTableSpec.getDimensionColumns(), List.of("name", "creation_time_millis", "id_sha",
        "friends", "properties__KEYS", "properties__VALUES"));
    Assertions.assertEquals(pinotTableSpec.getMetricColumns(),
        List.of("time_taken_millis"));
    Assertions.assertEquals(pinotTableSpec.getColumnsMaxLength(),
        Map.of("id_sha", 64));
    Assertions.assertEquals(pinotTableSpec.getInvertedIndexColumns(),
        List.of("friends", "properties__KEYS", "properties__VALUES"));

    Assertions.assertEquals(pinotTableSpec.getTableName(), "myView1");
    Assertions.assertEquals(pinotTableSpec.getLoadMode(), "MMAP");
    Assertions.assertEquals(pinotTableSpec.getNumReplicas(), 1);
    Assertions.assertEquals(pinotTableSpec.getRetentionTimeUnit(), "DAYS");
    Assertions.assertEquals(pinotTableSpec.getRetentionTimeValue(), "3");
    Assertions.assertEquals(pinotTableSpec.getBrokerTenant(), "defaultBroker");
    Assertions.assertEquals(pinotTableSpec.getServerTenant(), "defaultServer");
    Assertions.assertEquals(pinotTableSpec.getSegmentAssignmentStrategy(),
        "BalanceNumSegmentAssignmentStrategy");

    final Map<String, Object> streamConfigs = pinotTableSpec.getStreamConfigs();
    Assertions.assertEquals(streamConfigs.get("streamType"), "kafka");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.consumer.type"), "LowLevel");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.topic.name"), "test-view-events");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.consumer.factory.class.name"),
        "org.apache.pinot.plugin.stream.kafka20.KafkaConsumerFactory");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.decoder.class.name"),
        "org.apache.pinot.plugin.inputformat.avro.confluent.KafkaConfluentSchemaRegistryAvroMessageDecoder");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.decoder.prop.schema.registry.rest.url"), "http://localhost:8081");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.hlc.zk.connect.string"), "localhost:2181");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.zk.broker.url"), "localhost:2181");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.broker.list"), "localhost:9092");
    Assertions.assertEquals(streamConfigs.get("realtime.segment.flush.threshold.size"), "500000");
    Assertions.assertEquals(streamConfigs.get("realtime.segment.flush.threshold.time"), "3600000");
    Assertions.assertEquals(streamConfigs.get("stream.kafka.consumer.prop.auto.offset.reset"),
        "largest");
  }

  @Test
  public void testAvroDecoderSchemaIsAutoSetInTableCreationRequest() {
    ViewCreationSpec viewCreationSpec = createViewCreationSpecUsingConfig("sample-view-generation-spec-without-schema.conf");
    final Map<String, Object> streamConfigs = PinotUtils
        .getPinotTableSpecFromViewGenerationSpec(viewCreationSpec).getStreamConfigs();
    Assertions.assertEquals(streamConfigs.get("stream.kafka.decoder.prop.schema"),
        TestView.getClassSchema().toString());
  }

  @Test
  public void testKafkaSpecToString() {
    ViewCreationSpec.KafkaSpec kafkaSpec = new ViewCreationSpec.KafkaSpec();

    kafkaSpec.setBrokerAddress("broker:9000");
    kafkaSpec.setTopicName("test-topic");
    kafkaSpec.setPartitions(3);
    kafkaSpec.setReplicationFactor(10);

    Assertions.assertEquals("Topic Name: test-topic, Partitions: 3, ReplicationFactor: 10, BrokerAddress: broker:9000", kafkaSpec.toString());

    kafkaSpec.setBrokerAddress("broker:8090");
    kafkaSpec.setTopicName("test-topic-2");
    kafkaSpec.setPartitions(4);
    kafkaSpec.setReplicationFactor(7);

    Assertions.assertEquals("Topic Name: test-topic-2, Partitions: 4, ReplicationFactor: 7, BrokerAddress: broker:8090", String.valueOf(kafkaSpec));
  }

  @Test
  public void testMissingViewOutputSchemaClassConfig() {
    Assertions.assertThrows(
        ConfigException.Missing.class,
        () -> createViewCreationSpecUsingConfig("missing_view_output_schema_class.conf")
    );
  }

  @Test
  public void testMissingViewName() {
    Assertions.assertThrows(
        ConfigException.Missing.class,
        () -> createViewCreationSpecUsingConfig("missing_view_name.conf")
    );
  }

  private ViewCreationSpec createViewCreationSpecUsingConfig(String resourcePath) {
    File configFile = new File(this.getClass().getClassLoader().getResource(resourcePath).getPath());
    Config configs = ConfigFactory.parseFile(configFile);
    return ViewCreationSpec.parse(configs);
  }
}
