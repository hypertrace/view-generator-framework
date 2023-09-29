package org.hypertrace.core.viewgenerator;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.MULTI_VIEW_GEN_JOB_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.viewgenerator.service.MultiViewGeneratorLauncher;
import org.hypertrace.core.viewgenerator.test.api.RawServiceType;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeTwo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiViewGeneratorLauncherTest {
  private static final Logger logger =
      LoggerFactory.getLogger(MultiViewGeneratorLauncherTest.class);
  private MultiViewGeneratorLauncher underTest;
  private List<TestInputTopic<String, SpanTypeOne>> inputTopics = new ArrayList<>();
  private TestOutputTopic<String, SpanTypeTwo> spanTypeTwoOutputTopic;
  private TestOutputTopic<String, RawServiceType> rawServiceTypeOutputTopic;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-multi-view-generator")
  public void setUp() {
    underTest = new MultiViewGeneratorLauncher(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass()
                .getClassLoader()
                .getResource("configs/test-multi-view-generator/application.conf"));

    Map<String, Object> mergedProps = new HashMap<>();
    underTest.getBaseStreamsConfig().forEach(mergedProps::put);
    underTest.getStreamsConfig(config).forEach(mergedProps::put);
    mergedProps.put(MULTI_VIEW_GEN_JOB_CONFIG, config);

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Topology testTopology = streamsBuilder.build();
    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(testTopology, props);

    Serde<SpanTypeOne> spanTypeOneSerde = new AvroSerde<>();
    spanTypeOneSerde.configure(Map.of(), false);

    Serde<SpanTypeTwo> spanTypeTwoSerde = new AvroSerde<>();
    spanTypeTwoSerde.configure(Map.of(), false);

    Serde<RawServiceType> rawServiceTypeSerde = new AvroSerde<>();
    rawServiceTypeSerde.configure(Map.of(), false);

    // pick up from each view-gen config
    List<String> topics = List.of("test-input-topic1", "test-input-topic2");
    for (String topic : topics) {
      TestInputTopic<String, SpanTypeOne> inputTopic =
          td.createInputTopic(topic, new StringSerde().serializer(), spanTypeOneSerde.serializer());
      inputTopics.add(inputTopic);
    }

    spanTypeTwoOutputTopic =
        td.createOutputTopic(
            "test-span-type-two-output-topic",
            new StringSerde().deserializer(),
            spanTypeTwoSerde.deserializer());

    rawServiceTypeOutputTopic =
        td.createOutputTopic(
            "test-raw-service-type-output-topic",
            new StringSerde().deserializer(),
            rawServiceTypeSerde.deserializer());

    logger.info("test topology: {}", testTopology.describe());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-multi-view-generator")
  public void testMultiViewGeneratorTopologyWithMultipleInputStreams() {
    long spanStartTime = System.currentTimeMillis();
    long spanEndTime = spanStartTime + 1000;

    SpanTypeOne span =
        SpanTypeOne.newBuilder()
            .setSpanId("span-id")
            .setSpanKind("span-kind")
            .setStartTimeMillis(spanStartTime)
            .setEndTimeMillis(spanEndTime)
            .build();

    SpanTypeOne span1 =
        SpanTypeOne.newBuilder()
            .setSpanId("span-id-1")
            .setSpanKind("span-kind")
            .setStartTimeMillis(spanStartTime)
            .setEndTimeMillis(spanEndTime)
            .build();

    // test-view-gen-span-event consumes from both the topics, so for both pipeInput
    // it should receive the result.
    // test-view-gen-raw-service consume only from one input topic, so it should
    // get only one piped output.
    inputTopics.get(0).pipeInput(null, span);
    KeyValue<String, SpanTypeTwo> spanTypeTwoKeyValue = spanTypeTwoOutputTopic.readKeyValue();
    assertNull(spanTypeTwoKeyValue.key, "null key expected");
    assertEquals("span-id", spanTypeTwoKeyValue.value.getSpanId());
    assertEquals("span-kind", spanTypeTwoKeyValue.value.getSpanKind());
    assertEquals(spanStartTime, spanTypeTwoKeyValue.value.getStartTimeMillis());
    assertEquals(spanEndTime, spanTypeTwoKeyValue.value.getEndTimeMillis());

    KeyValue<String, RawServiceType> rawServiceTypeKeyValue =
        rawServiceTypeOutputTopic.readKeyValue();
    assertNull(rawServiceTypeKeyValue.key, "null key expected");
    assertEquals("span-id", rawServiceTypeKeyValue.value.getSpanId());
    assertEquals("span-kind", rawServiceTypeKeyValue.value.getSpanKind());
    assertEquals(spanStartTime, rawServiceTypeKeyValue.value.getStartTimeMillis());
    assertEquals(spanEndTime, rawServiceTypeKeyValue.value.getEndTimeMillis());

    inputTopics.get(1).pipeInput(null, span1);

    spanTypeTwoKeyValue = spanTypeTwoOutputTopic.readKeyValue();
    assertNull(rawServiceTypeKeyValue.key, "null key expected");
    assertEquals("span-id-1", spanTypeTwoKeyValue.value.getSpanId());
    assertEquals("span-kind", spanTypeTwoKeyValue.value.getSpanKind());
    assertEquals(spanStartTime, spanTypeTwoKeyValue.value.getStartTimeMillis());
    assertEquals(spanEndTime, spanTypeTwoKeyValue.value.getEndTimeMillis());

    assertTrue(rawServiceTypeOutputTopic.isEmpty());

    // test by sending non-null key
    inputTopics.get(0).pipeInput("t1", span);
    inputTopics.get(1).pipeInput("t2", span1);

    // two records for span-event, one record for raw-service type
    spanTypeTwoKeyValue = spanTypeTwoOutputTopic.readKeyValue();
    assertNotNull(spanTypeTwoKeyValue.key, "non null key expected");
    assertTrue(List.of("t1", "t2").contains(spanTypeTwoKeyValue.key));

    spanTypeTwoKeyValue = spanTypeTwoOutputTopic.readKeyValue();
    assertNotNull(spanTypeTwoKeyValue.key, "non null key expected");
    assertTrue(List.of("t1", "t2").contains(spanTypeTwoKeyValue.key));

    rawServiceTypeKeyValue = rawServiceTypeOutputTopic.readKeyValue();
    assertNotNull(rawServiceTypeKeyValue.key, "non null key expected");
    assertEquals("t1", rawServiceTypeKeyValue.key);
  }
}
