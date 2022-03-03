package org.hypertrace.core.viewgenerator;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.*;
import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.viewgenerator.service.ViewGeneratorLauncher;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeTwo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class ViewGeneratorLauncherTest {
  private static final String SERVICE_NAME = "servicename";
  private ViewGeneratorLauncher underTest;
  TestInputTopic<byte[], SpanTypeOne> inputTopic;
  TestOutputTopic<byte[], SpanTypeTwo> outputTopic;

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

    inputTopic =
        td.createInputTopic(
            config.getString(INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            spanTypeOneSerde.serializer());
    outputTopic =
        td.createOutputTopic(
            config.getString(OUTPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().deserializer(),
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

    inputTopic.pipeInput(null, span);

    KeyValue<byte[], SpanTypeTwo> kv = outputTopic.readKeyValue();
    assertNull(kv.key, "null key expected");
    assertEquals("span-id", kv.value.getSpanId());
    assertEquals("span-kind", kv.value.getSpanKind());
    assertEquals(spanStartTime, kv.value.getStartTimeMillis());
    assertEquals(spanEndTime, kv.value.getEndTimeMillis());
  }
}
