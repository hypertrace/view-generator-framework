package org.hypertrace.core.viewgenerator;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.INPUT_TOPIC_CONFIG_KEY;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hypertrace.core.kafkastreams.framework.serdes.AvroSerde;
import org.hypertrace.core.serviceframework.config.ConfigClientFactory;
import org.hypertrace.core.viewgenerator.service.ViewGeneratorLauncher;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

public class ViewGeneratorLauncherTest {
  private static final String SERVICE_NAME = "servicename";
  private ViewGeneratorLauncher underTest;

  @BeforeEach
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-view-generator")
  public void setUp() {
    underTest = new ViewGeneratorLauncher(ConfigClientFactory.getClient());
  }

  @Test
  @SetEnvironmentVariable(key = "SERVICE_NAME", value = "test-view-generator")
  public void whenJaegerSpansAreProcessedExpectRawSpansToBeOutput() {
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
    TestInputTopic<byte[], SpanTypeOne> inputTopic =
        td.createInputTopic(
            config.getString(INPUT_TOPIC_CONFIG_KEY),
            Serdes.ByteArray().serializer(),
            new JaegerSpanSerde().serializer());

    Serde<RawSpan> rawSpanSerde = new AvroSerde<>();
    rawSpanSerde.configure(Map.of(), false);

    Serde<TraceIdentity> spanIdentitySerde = new AvroSerde<>();
    spanIdentitySerde.configure(Map.of(), true);

    TestOutputTopic outputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            rawSpanSerde.deserializer());

    TestOutputTopic rawLogOutputTopic =
        td.createOutputTopic(
            config.getString(SpanNormalizerConstants.OUTPUT_TOPIC_RAW_LOGS_CONFIG_KEY),
            spanIdentitySerde.deserializer(),
            new AvroSerde<>().deserializer());

    Span span =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("1".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-1".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(5).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("e1")
                            .setVStr("some event detail")
                            .build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("e2")
                            .setVStr("some event detail")
                            .build()))
            .addLogs(
                Log.newBuilder()
                    .setTimestamp(Timestamp.newBuilder().setSeconds(10).build())
                    .addFields(
                        JaegerSpanInternalModel.KeyValue.newBuilder()
                            .setKey("z2")
                            .setVStr("some event detail")
                            .build()))
            .build();
    inputTopic.pipeInput(span);

    KeyValue<TraceIdentity, RawSpan> kv = outputTopic.readKeyValue();
    assertEquals("__default", kv.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-1".getBytes()).toByteArray()),
        HexUtils.getHex(kv.key.getTraceId().array()));
    RawSpan value = kv.value;
    assertEquals(HexUtils.getHex("1".getBytes()), HexUtils.getHex((value).getEvent().getEventId()));
    assertEquals(SERVICE_NAME, value.getEvent().getServiceName());

    KeyValue<String, LogEvents> keyValue = rawLogOutputTopic.readKeyValue();
    LogEvents logEvents = keyValue.value;
    Assertions.assertEquals(2, logEvents.getLogEvents().size());

    // pipe in one more span which doesn't match spanDropFilters
    Span span2 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("2".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-2".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .build();

    inputTopic.pipeInput(span2);
    KeyValue<TraceIdentity, RawSpan> kv1 = outputTopic.readKeyValue();
    assertNotNull(kv1);
    assertEquals("__default", kv1.key.getTenantId());
    assertEquals(
        HexUtils.getHex(ByteString.copyFrom("trace-2".getBytes()).toByteArray()),
        HexUtils.getHex(kv1.key.getTraceId().array()));

    // pipe in one more span which match one of spanDropFilters (http.method & http.url)
    Span span3 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.method")
                    .setVStr("GET")
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("http.url")
                    .setVStr("http://xyz.com/health/check")
                    .build())
            .build();

    inputTopic.pipeInput(span3);
    assertTrue(outputTopic.isEmpty());

    // pipe in one more span which match one of spanDropFilters (grpc.url)
    Span span4 =
        Span.newBuilder()
            .setSpanId(ByteString.copyFrom("3".getBytes()))
            .setTraceId(ByteString.copyFrom("trace-3".getBytes()))
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("jaeger.servicename")
                    .setVStr(SERVICE_NAME)
                    .build())
            .addTags(
                JaegerSpanInternalModel.KeyValue.newBuilder()
                    .setKey("grpc.url")
                    .setVStr("doesn't match with input filter set")
                    .build())
            .build();

    inputTopic.pipeInput(span4);
    assertTrue(outputTopic.isEmpty());
  }
}
