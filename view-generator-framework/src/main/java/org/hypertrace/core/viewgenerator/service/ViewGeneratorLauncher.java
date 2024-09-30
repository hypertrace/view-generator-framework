package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.INPUT_TOPICS_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.api.ViewGeneratorConstants.VIEW_GENERATOR_CLASS_CONFIG_KEY;

import com.google.common.base.Splitter;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.hypertrace.core.kafkastreams.framework.KafkaStreamsApp;
import org.hypertrace.core.kafkastreams.framework.partitioner.AvroFieldValuePartitioner;
import org.hypertrace.core.kafkastreams.framework.partitioner.GroupPartitionerBuilder;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.viewgenerator.api.ClientRegistry;
import org.hypertrace.core.viewgenerator.api.DefaultClientRegistry;
import org.hypertrace.core.viewgenerator.api.ViewGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ViewGeneratorLauncher<IN extends SpecificRecord, OUT extends GenericRecord>
    extends KafkaStreamsApp {
  private static final Logger logger = LoggerFactory.getLogger(ViewGeneratorLauncher.class);

  private final ClientRegistry clientRegistry;
  private String viewGenName;

  public ViewGeneratorLauncher(ConfigClient configClient) {
    this(configClient, null);
  }

  public ViewGeneratorLauncher(ConfigClient configClient, ClientRegistry clientRegistry) {
    super(configClient);
    if (clientRegistry != null) {
      this.clientRegistry = clientRegistry;
    } else {
      this.clientRegistry = new DefaultClientRegistry(super.getGrpcChannelRegistry());
    }
  }

  public String getViewGenName() {
    return viewGenName;
  }

  public void setViewGenName(String viewGenName) {
    this.viewGenName = viewGenName;
  }

  @Override
  public StreamsBuilder buildTopology(
      Map<String, Object> streamProps,
      StreamsBuilder streamsBuilder,
      Map<String, KStream<?, ?>> inputStreams) {

    Config jobConfig = getJobConfig(streamProps);

    List<String> inputTopics = jobConfig.getStringList(INPUT_TOPICS_CONFIG_KEY);
    String outputTopic = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);

    KStream<String, IN> mergedStream = null;
    int mergedStreamId = 0;
    for (String topic : inputTopics) {
      KStream<String, IN> inputStream = (KStream<String, IN>) inputStreams.get(topic);

      if (inputStream == null) {
        inputStream =
            streamsBuilder.stream(
                topic,
                Consumed.with(Serdes.String(), (Serde<IN>) null).withName("source-" + topic));
        inputStreams.put(topic, inputStream);
      }

      if (mergedStream == null) {
        mergedStream = inputStream;
      } else {
        mergedStream =
            mergedStream.merge(
                inputStream, Named.as("merged-stream-" + getViewGenName() + "-" + mergedStreamId));
        mergedStreamId++;
      }
    }

    // This environment property helps in overriding producer value serde. For hypertrace quickstart
    // deployment, this helps in using GenericAvroSerde for pinot views.
    Serde<OUT> producerValueSerde = null;
    String envProducerValueSerdeClassName = System.getenv("PRODUCER_VALUE_SERDE");
    if (envProducerValueSerdeClassName != null) {
      try {
        logger.info("Using producer value serde: {}", envProducerValueSerdeClassName);
        Class<Serde<OUT>> clazz = (Class<Serde<OUT>>) Class.forName(envProducerValueSerdeClassName);
        producerValueSerde = clazz.getDeclaredConstructor().newInstance();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
    ViewGenerator<IN, OUT> viewGenerator = createViewGenerator(jobConfig);

    Objects.requireNonNull(mergedStream)
        .process(
            () -> new ViewGenerationProcessor<>(viewGenerator),
            Named.as("processor-" + getViewGenName()))
        .to(
            outputTopic,
            Produced.with(Serdes.String(), producerValueSerde, viewGenerator.getPartitioner())
                .withName("sink-" + outputTopic));
    return streamsBuilder;
  }

  @Override
  public String getJobConfigKey() {
    String jobConfigKey = getViewGenName();
    return jobConfigKey != null ? jobConfigKey : DEFAULT_VIEW_GEN_JOB_CONFIG_KEY;
  }

  private Config getJobConfig(Map<String, Object> properties) {
    return (Config) properties.get(getJobConfigKey());
  }

  private StreamPartitioner<Object, OUT> getPartitioner(
      Config jobConfig, Map<String, Object> streamProps, String topic) {
    boolean useNewPartitioner = false;
    if (jobConfig.hasPath("use.new.partitioner"))
      useNewPartitioner = jobConfig.getBoolean("use.new.partitioner");

    if (useNewPartitioner) {
      return new GroupPartitionerBuilder<Object, OUT>()
          .buildPartitioner(
              "spans",
              jobConfig,
              (nullKey, trace) -> null, // key,
              null, // delegate partitioner,
              null // channel reqistry
              );
    } else {
      String topicsStr = (String) streamProps.get("avro.field.value.partitioner.enabled.topics");
      if (topicsStr != null) {
        boolean enabledForTopic =
            Splitter.on(",").trimResults().splitToStream(topicsStr).anyMatch(topic::equals);
        if (enabledForTopic) {
          return new AvroFieldValuePartitioner<>(streamProps);
        }
      }
    }
    return null;
  }

  private ViewGenerator<IN, OUT> createViewGenerator(Config jobConfig) {
    String viewGenClassName = jobConfig.getString(VIEW_GENERATOR_CLASS_CONFIG_KEY);
    ViewGenerator<IN, OUT> viewGenerator;
    try {
      viewGenerator =
          (ViewGenerator) Class.forName(viewGenClassName).getDeclaredConstructor().newInstance();
      viewGenerator.configure(jobConfig, clientRegistry);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return viewGenerator;
  }
}
