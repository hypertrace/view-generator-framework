package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGenerationJob.JobConfig.FLINK_SINK_CONFIG_PATH;
import static org.hypertrace.core.viewgenerator.service.ViewGenerationJob.JobConfig.LOG_FAILURES_CONFIG;
import static org.hypertrace.core.viewgenerator.service.ViewGenerationJob.JobConfig.VIEW_GENERATOR_CLASS_CONFIG;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.hypertrace.core.flinkutils.avro.RegistryBasedAvroSerde;
import org.hypertrace.core.flinkutils.utils.FlinkUtils;
import org.hypertrace.core.serviceframework.config.ConfigUtils;

public class ViewGenerationJob {
  private final StreamExecutionEnvironment environment;
  private final Config configs;
  private String inputTopic;
  private String outputTopic;
  private Properties kafkaConsumerConfig;
  private Properties kafkaProducerConfig;
  private Map<String, String> sourceSchemaRegistryConfig;
  private Map<String, String> sinkSchemaRegistryConfig;
  private boolean logFailuresOnly;
  private ViewGenerationProcessFunction<? extends SpecificRecord, ? extends GenericRecord> processFunction;

  ViewGenerationJob(StreamExecutionEnvironment environment, Config configs) {
    this.environment = environment;
    this.configs = configs;
  }

  public void init() {
    try {
      Config flinkSourceConfig = configs.getConfig(JobConfig.FLINK_SOURCE_CONFIG_PATH);
      this.inputTopic = flinkSourceConfig.getString(JobConfig.TOPIC_NAME_CONFIG);
      this.kafkaConsumerConfig = ConfigUtils
          .getPropertiesConfig(flinkSourceConfig, JobConfig.KAFKA_CONFIG_PATH);
      this.sourceSchemaRegistryConfig = ConfigUtils
          .getFlatMapConfig(flinkSourceConfig, JobConfig.SCHEMA_REGISTRY_CONFIG_PATH);

      Config flinkSinkConfig = configs.getConfig(FLINK_SINK_CONFIG_PATH);
      this.outputTopic = flinkSinkConfig.getString(JobConfig.TOPIC_NAME_CONFIG);
      this.logFailuresOnly = flinkSinkConfig.getBoolean(LOG_FAILURES_CONFIG);
      this.kafkaProducerConfig = ConfigUtils
          .getPropertiesConfig(flinkSinkConfig, JobConfig.KAFKA_CONFIG_PATH);
      this.sinkSchemaRegistryConfig = ConfigUtils
          .getFlatMapConfig(flinkSinkConfig, JobConfig.SCHEMA_REGISTRY_CONFIG_PATH);

      String viewGeneratorClassConfig = configs.getString(VIEW_GENERATOR_CLASS_CONFIG);
      this.processFunction = new ViewGenerationProcessFunction<>(viewGeneratorClassConfig);
      String inputClassName = flinkSourceConfig.getString(JobConfig.INPUT_CLASS);
      Class<?> inputClass = getInputClass(inputClassName);

      FlinkKafkaConsumer<?> structuredTraceKafkaConsumer =
          FlinkUtils.createKafkaConsumer(inputTopic,
              new RegistryBasedAvroSerde(inputTopic, inputClass, sourceSchemaRegistryConfig),
              kafkaConsumerConfig);
      DataStream<?> inputStream = environment.addSource(structuredTraceKafkaConsumer).name("Kafka/"+ inputTopic);
      final SingleOutputStreamOperator<SerializationSchema> outputStream =
          inputStream.process((ProcessFunction) processFunction).returns(processFunction.getViewClass());

      outputStream.addSink(FlinkUtils.getFlinkKafkaProducer(outputTopic,
          new RegistryBasedAvroSerde(outputTopic, processFunction.getViewClass(),
              sinkSchemaRegistryConfig),
          null/* By specifying null this will default to kafka producer's default partitioner i.e. round-robin*/,
          kafkaProducerConfig, logFailuresOnly)).name("Kafka/" + this.outputTopic);
    } catch (ConfigException.Missing e) {
      throw new RuntimeException("Required config missing.", e);
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize view generator.", e);
    }
  }

  private Class<?> getInputClass(String inputClassName) {
    try {
      return Class.forName(inputClassName);
    } catch (Exception ex) {
      throw new RuntimeException("Failed to get InputClass from flinkSourceConfig", ex);
    }
  }

  public void stop() {
  }

  protected static class JobConfig {
    public static final String FLINK_SOURCE_CONFIG_PATH = "flink.source";
    public static final String FLINK_SINK_CONFIG_PATH = "flink.sink";
    public static final String TOPIC_NAME_CONFIG = "topic";
    public static final String KAFKA_CONFIG_PATH = "kafka";
    public static final String SCHEMA_REGISTRY_CONFIG_PATH = "schema.registry";
    public static final String LOG_FAILURES_CONFIG = "log.failures.only";
    public static final String FLINK_JOB = "flink.job";
    public static final String METRICS = "metrics";
    public static final String PARALLELISM = "parallelism";
    public static final String INPUT_CLASS = "input.class";
    public static final String VIEW_GENERATOR_CLASS_CONFIG = "view.generator.class";
  }
}
