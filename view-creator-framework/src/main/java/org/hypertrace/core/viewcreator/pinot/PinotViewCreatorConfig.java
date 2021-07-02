package org.hypertrace.core.viewcreator.pinot;

public class PinotViewCreatorConfig {

  /////////////
  // Pinot schema configurations
  /////////////
  public static final String PINOT_SCHEMA_MAP_KEYS_SUFFIX = "__KEYS";
  public static final String PINOT_SCHEMA_MAP_VALUES_SUFFIX = "__VALUES";

  /////////////
  // Pinot table configurations (common for both REALTIME & OFFLINE)
  /////////////
  public static final String PINOT_CONFIGS_KEY = "pinot";

  /////////////
  // Pinot ingestion transform configurations
  /////////////
  public static final String PINOT_TRANSFORM_COLUMN_NAME = "columnName";
  public static final String PINOT_TRANSFORM_COLUMN_FUNCTION = "transformFunction";

  /////////////
  // Pinot REALTIME table configurations
  /////////////
  public static final String PINOT_REALTIME_CONFIGS_KEY = "pinotRealtime";
  public static final String PINOT_STREAM_CONFIGS_KEY = "streamConfigs";
  public static final String STREAM_KAFKA_DECODER_CLASS_NAME_KEY =
      "stream.kafka.decoder.class.name";
  public static final String STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY =
      "stream.kafka.decoder.prop.schema";
  public static final String STREAM_KAFKA_CONSUMER_TYPE_KEY = "stream.kafka.consumer.type";
  public static final String SIMPLE_AVRO_MESSAGE_DECODER =
      "org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder";

  /////////////
  // Pinot OFFLINE table configurations
  /////////////
  public static final String PINOT_OFFLINE_CONFIGS_KEY = "pinotOffline";

  /////////////
  // REST API
  /////////////
  public static final String PINOT_REST_URI_TABLES = "tables";
}
