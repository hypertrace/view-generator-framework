package org.hypertrace.core.viewgenerator;

import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.INPUT_TOPICS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.viewgenerator.service.ViewGeneratorLauncher;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeTwo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class ViewGeneratorLauncherTest {
  private ViewGeneratorLauncher underTest;
  private List<TestInputTopic<String, SpanTypeOne>> inputTopics = new ArrayList<>();
  private TestOutputTopic<String, SpanTypeTwo> outputTopic;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-view-generator")
  public void setUp() {
    underTest = new ViewGeneratorLauncher(ConfigClientFactory.getClient());
    Config config =
        ConfigFactory.parseURL(
            getClass()
                .getClassLoader()
                .getResource("configs/test-view-generator/application.conf"));

    Map<String, Object> mergedProps = new HashMap<>();
    underTest.getBaseStreamsConfig().forEach(mergedProps::put);
    underTest.getStreamsConfig(config).forEach(mergedProps::put);
    mergedProps.put(DEFAULT_VIEW_GEN_JOB_CONFIG_KEY, config);

    StreamsBuilder streamsBuilder =
        underTest.buildTopology(mergedProps, new StreamsBuilder(), new HashMap<>());

    Properties props = new Properties();
    mergedProps.forEach(props::put);

    TopologyTestDriver td = new TopologyTestDriver(streamsBuilder.build(), props);

    Serde<SpanTypeOne> spanTypeOneSerde = new AvroSerde<>();
    spanTypeOneSerde.configure(Map.of(), false);

    Serde<SpanTypeTwo> spanTypeTwoSerde = new AvroSerde<>();
    spanTypeTwoSerde.configure(Map.of(), false);

    List<String> topics = config.getStringList(INPUT_TOPICS_CONFIG_KEY);
    for (String topic : topics) {
      TestInputTopic<String, SpanTypeOne> inputTopic =
          td.createInputTopic(topic, new StringSerde().serializer(), spanTypeOneSerde.serializer());
      inputTopics.add(inputTopic);
    }
    outputTopic =
        td.createOutputTopic(
            config.getString(OUTPUT_TOPIC_CONFIG_KEY),
            new StringSerde().deserializer(),
            spanTypeTwoSerde.deserializer());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-view-generator")
  public void testViewGeneratorTopology() {
    long spanStartTime = System.currentTimeMillis();
    long spanEndTime = spanStartTime + 1000;

    SpanTypeOne span =
        SpanTypeOne.newBuilder()
            .setSpanId("span-id")
            .setSpanKind("span-kind")
            .setStartTimeMillis(spanStartTime)
            .setEndTimeMillis(spanEndTime)
            .build();

    Serde<SpanTypeOne> spanTypeOneSerde = new AvroSerde<>();
    spanTypeOneSerde.configure(Map.of(), false);

    inputTopics.get(0).pipeInput(null, span);

    KeyValue<String, SpanTypeTwo> kv = outputTopic.readKeyValue();
    assertNull(kv.key, "non-null key expected");
    assertEquals("span-id", kv.value.getSpanId());
    assertEquals("span-kind", kv.value.getSpanKind());
    assertEquals(spanStartTime, kv.value.getStartTimeMillis());
    assertEquals(spanEndTime, kv.value.getEndTimeMillis());

    inputTopics.get(0).pipeInput("t1", span);

    kv = outputTopic.readKeyValue();
    assertNotNull(kv.key, "non null key expected");
    assertEquals("span-id", kv.value.getSpanId());
    assertEquals("span-kind", kv.value.getSpanKind());
    assertEquals(spanStartTime, kv.value.getStartTimeMillis());
    assertEquals(spanEndTime, kv.value.getEndTimeMillis());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-view-generator")
  public void testViewGeneratorTopologyWithMultipleInputStreams() {
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

    Serde<SpanTypeOne> spanTypeOneSerde = new AvroSerde<>();
    spanTypeOneSerde.configure(Map.of(), false);

    inputTopics.get(0).pipeInput(null, span);

    KeyValue<String, SpanTypeTwo> kv = outputTopic.readKeyValue();
    assertNull(kv.key, "null key expected");
    assertEquals("span-id", kv.value.getSpanId());
    assertEquals("span-kind", kv.value.getSpanKind());
    assertEquals(spanStartTime, kv.value.getStartTimeMillis());
    assertEquals(spanEndTime, kv.value.getEndTimeMillis());

    inputTopics.get(1).pipeInput(null, span1);
    kv = outputTopic.readKeyValue();
    assertNull(kv.key, "null key expected");
    assertEquals("span-id-1", kv.value.getSpanId());
    assertEquals("span-kind", kv.value.getSpanKind());
    assertEquals(spanStartTime, kv.value.getStartTimeMillis());
    assertEquals(spanEndTime, kv.value.getEndTimeMillis());
  }

  public static void main(String[] args) {
    String sessionid = "81e1727b25ad8b6256f42171987a23e71f5c7aec982d6f553370a97a937cbadf_1";

    System.out.println(Utils.toPositive(Utils.murmur2(sessionid.getBytes())) % 48);

  }
}
