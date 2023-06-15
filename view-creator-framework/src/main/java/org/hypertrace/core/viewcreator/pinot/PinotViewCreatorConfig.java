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
  // Pinot ingestion filter configurations
  /////////////
  public static final String PINOT_FILTER_FUNCTION = "filterFunction";

  /////////////
  // Pinot tier configurations
  /////////////
  public static final String PINOT_TIER_NAME = "name";
  public static final String PINOT_TIER_SEGMENT_SELECTOR_TYPE = "segmentSelectorType";
  public static final String PINOT_TIER_SEGMENT_AGE = "segmentAge";
  public static final String PINOT_TIER_STORAGE_TYPE = "storageType";
  public static final String PINOT_TIER_SERVER_TAG = "serverTag";

  /////////////
  // Pinot REALTIME table configurations
  /////////////
  public static final String PINOT_REALTIME_CONFIGS_KEY = "pinotRealtime";
  public static final String PINOT_RT_CONSUMING_TAG_KEY = "realtimeConsuming";
  public static final String PINOT_RT_COMPLETED_TAG_KEY = "realtimeCompleted";
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

  // text index config keys
  public static final String TEXT_INDEX_CONFIG_COLUMN = "column";
  public static final String TEXT_INDEX_CONFIG_SKIP_PRIOR_SEGMENTS = "skipPriorSegments";

  // completion config keys
  public static final String COMPLETION_CONFIG_COMPLETION_MODE = "completionMode";
}
