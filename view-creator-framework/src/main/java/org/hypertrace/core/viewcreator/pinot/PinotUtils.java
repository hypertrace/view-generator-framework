package org.hypertrace.core.viewcreator.pinot;

import static com.google.common.collect.Maps.newHashMap;
import static java.net.HttpURLConnection.HTTP_CONFLICT;
import static java.net.HttpURLConnection.HTTP_OK;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_CONFIGS_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_OFFLINE_CONFIGS_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_REALTIME_CONFIGS_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_REST_URI_TABLES;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_RT_COMPLETED_TAG_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_RT_CONSUMING_TAG_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_SCHEMA_MAP_KEYS_SUFFIX;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_SCHEMA_MAP_VALUES_SUFFIX;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_STREAM_CONFIGS_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TIER_NAME;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TIER_SEGMENT_AGE;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TIER_SEGMENT_SELECTOR_TYPE;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TIER_SERVER_TAG;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TIER_STORAGE_TYPE;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TRANSFORM_COLUMN_FUNCTION;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.PINOT_TRANSFORM_COLUMN_NAME;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.SIMPLE_AVRO_MESSAGE_DECODER;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.STREAM_KAFKA_CONSUMER_TYPE_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.STREAM_KAFKA_DECODER_CLASS_NAME_KEY;
import static org.hypertrace.core.viewcreator.pinot.PinotViewCreatorConfig.STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;
import com.typesafe.config.ConfigValue;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableTaskConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.config.table.TagOverrideConfig;
import org.apache.pinot.spi.config.table.TierConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.TransformConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeFormatUnitSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AddSchemaCommand;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotUtils.class);

  public static PinotTableSpec getPinotRealtimeTableSpec(ViewCreationSpec creationSpec) {
    final Config viewCreatorConfig = creationSpec.getViewCreatorConfig();
    final Config pinotRealtimeConfig =
        getOptionalConfig(viewCreatorConfig, PINOT_REALTIME_CONFIGS_KEY)
            .withFallback(viewCreatorConfig.getConfig(PINOT_CONFIGS_KEY));
    final PinotTableSpec pinotTableSpec =
        ConfigBeanFactory.create(pinotRealtimeConfig, PinotTableSpec.class);

    // ConfigBeanFactory will parse Map to nested structure. Try to reset streamConfigs as a
    // flattened map.
    Map<String, Object> streamConfigsMap =
        newHashMap(ConfigUtils.getFlatMapConfig(pinotRealtimeConfig, PINOT_STREAM_CONFIGS_KEY));
    if (SIMPLE_AVRO_MESSAGE_DECODER.equals(
        streamConfigsMap.get(STREAM_KAFKA_DECODER_CLASS_NAME_KEY))) {

      // Try to infer output schema from ViewCreationSpec if not provided.
      if (!streamConfigsMap.containsKey(STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY)) {
        streamConfigsMap.put(
            STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY, creationSpec.getOutputSchema().toString());
      }
    }
    pinotTableSpec.setStreamConfigs(streamConfigsMap);

    return pinotTableSpec;
  }

  public static PinotTableSpec getPinotOfflineTableSpec(ViewCreationSpec creationSpec) {
    final Config viewCreatorConfig = creationSpec.getViewCreatorConfig();
    final Config pinotOfflineConfig =
        getOptionalConfig(viewCreatorConfig, PINOT_OFFLINE_CONFIGS_KEY)
            .withFallback(viewCreatorConfig.getConfig(PINOT_CONFIGS_KEY));
    return ConfigBeanFactory.create(pinotOfflineConfig, PinotTableSpec.class);
  }

  public static Schema createPinotSchema(
      ViewCreationSpec viewCreationSpec, PinotTableSpec pinotTableSpec) {
    String schemaName = viewCreationSpec.getViewName();
    org.apache.avro.Schema avroSchema = viewCreationSpec.getOutputSchema();
    TimeUnit timeUnit = pinotTableSpec.getTimeUnit();
    String timeColumn = pinotTableSpec.getTimeColumn();
    List<String> dimensionColumns = pinotTableSpec.getDimensionColumns();
    List<String> metricColumns = pinotTableSpec.getMetricColumns();
    List<String> dateTimeColumns = pinotTableSpec.getDateTimeColumns();
    Map<String, Object> columnsMaxLength = pinotTableSpec.getColumnsMaxLength();
    return convertAvroSchemaToPinotSchema(
        avroSchema,
        schemaName,
        timeColumn,
        timeUnit,
        dimensionColumns,
        metricColumns,
        dateTimeColumns,
        columnsMaxLength);
  }

  public static boolean uploadPinotSchema(PinotTableSpec pinotTableSpec, final Schema schema) {
    return uploadPinotSchema(
        pinotTableSpec.getControllerHost(), pinotTableSpec.getControllerPort(), schema);
  }

  public static boolean uploadPinotSchema(
      String controllerHost, String controllerPort, final Schema pinotSchemaFromAvroSchema) {
    File tmpFile = null;
    try {
      tmpFile =
          File.createTempFile(
              String.format("temp-schema-%s-", pinotSchemaFromAvroSchema.getSchemaName()), ".json");
      LOGGER.info("Created a temp schema file {}", tmpFile.getAbsolutePath());
      BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile));
      writer.write(pinotSchemaFromAvroSchema.toPrettyJsonString());
      writer.flush();
      writer.close();
      return new AddSchemaCommand()
          .setControllerHost(controllerHost)
          .setControllerPort(controllerPort)
          .setSchemaFilePath(tmpFile.getPath())
          .setExecute(true)
          .execute();
    } catch (Exception e) {
      LOGGER.error("Failed to upload Pinot schema.", e);
      return false;
    } finally {
      tmpFile.delete();
    }
  }

  public static Schema convertAvroSchemaToPinotSchema(
      org.apache.avro.Schema avroSchema,
      String name,
      String timeColumn,
      TimeUnit timeUnit,
      List<String> dimensionColumns,
      List<String> metricColumns,
      List<String> dateTimeColumns,
      Map<String, Object> columnsMaxLength) {
    Schema pinotSchema = new Schema();
    pinotSchema.setSchemaName(name);

    for (Field field : avroSchema.getFields()) {
      FieldSpec fieldSpec;
      // TODO: move this to AvroUtils in Pinot-tools for map
      List<FieldSpec> fieldSpecs = new ArrayList<>();
      if (field.schema().getType().equals(Type.MAP)) {

        Pair<Field, Field> keyToValueField = splitMapField(field);
        Field key = keyToValueField.getKey();
        Field value = keyToValueField.getValue();
        // only support single value for now
        fieldSpec =
            new DimensionFieldSpec(
                key.name(), AvroUtils.extractFieldDataType(key), AvroUtils.isSingleValueField(key));
        // adds the key here first
        fieldSpecs.add(fieldSpec);

        fieldSpec =
            new DimensionFieldSpec(
                value.name(),
                AvroUtils.extractFieldDataType(value),
                AvroUtils.isSingleValueField(value));
        fieldSpecs.add(fieldSpec);
      } else if (timeColumn.equals(field.name())) {
        validateDateTimeColumn(field, timeUnit);
        fieldSpec =
            new DateTimeFieldSpec(
                field.name(),
                AvroUtils.extractFieldDataType(field),
                new DateTimeFormatSpec(1, timeUnit.name(), TimeFormat.EPOCH.name()).getFormat(),
                new DateTimeGranularitySpec(1, timeUnit).getGranularity());
        fieldSpecs.add(fieldSpec);
      } else if (dateTimeColumns.contains(field.name())) {
        // No way to specify unit type or rich time formats. So, hard coding all date time columns
        // to be millis
        validateDateTimeColumn(field, TimeUnit.MILLISECONDS);
        fieldSpec =
            new DateTimeFieldSpec(
                field.name(),
                AvroUtils.extractFieldDataType(field),
                new DateTimeFormatSpec(
                        1,
                        new DateTimeFormatUnitSpec(TimeUnit.MILLISECONDS.toString())
                            .getDateTimeTransformUnit()
                            .name(),
                        TimeFormat.EPOCH.name())
                    .getFormat(),
                new DateTimeGranularitySpec(1, TimeUnit.MILLISECONDS).getGranularity());
        fieldSpecs.add(fieldSpec);
      } else if (dimensionColumns.contains(field.name())) {
        fieldSpec =
            new DimensionFieldSpec(
                field.name(),
                AvroUtils.extractFieldDataType(field),
                AvroUtils.isSingleValueField(field));
        fieldSpecs.add(fieldSpec);
      } else if (metricColumns.contains(field.name())) {
        fieldSpec = new MetricFieldSpec(field.name(), AvroUtils.extractFieldDataType(field));
        fieldSpecs.add(fieldSpec);
      }

      for (FieldSpec convertedSpec : fieldSpecs) {
        if (convertedSpec != null) {
          Object defaultVal = field.defaultVal();
          // Replace the avro generated defaults in certain cases
          if (field.schema().getType().equals(Type.MAP)) {
            // A map is split into two multivalued string cols, use an empty string default for each
            // TODO - why not use null and let pinot decide?
            defaultVal = "";
          } else if (defaultVal == JsonProperties.NULL_VALUE) {
            defaultVal = null;
          } else if (!AvroUtils.isSingleValueField(field)
              && defaultVal instanceof Collection
              && ((Collection<?>) defaultVal).isEmpty()) {
            // Convert an empty collection into a null for a multivalued col
            defaultVal = null;
          }
          if (defaultVal != null) {
            convertedSpec.setDefaultNullValue(defaultVal);
          }
          Object maxLength = columnsMaxLength.get(convertedSpec.getName());
          if (maxLength != null) {
            convertedSpec.setMaxLength((Integer) maxLength);
          }
          pinotSchema.addField(convertedSpec);
        }
      }
    }
    return pinotSchema;
  }

  private static void validateDateTimeColumn(Field field, TimeUnit timeUnit) {
    // In the current model, we don't have the flexibility to support/define other types or
    // granularity. So, we can support only LONG types.
    final DataType pinotDataType = AvroUtils.extractFieldDataType(field);
    if (pinotDataType != DataType.LONG) {
      throw new RuntimeException(
          "Unsupported type for a date time column. column: "
              + field.name()
              + ". Expected: "
              + Type.LONG
              + ", Actual: "
              + field.schema().getType());
    }
  }

  public static Pair<Field, Field> splitMapField(Field mapField) {
    org.apache.avro.Schema keysFieldSchema =
        org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.STRING));
    Field keysField =
        new Field(
            mapField.name() + PINOT_SCHEMA_MAP_KEYS_SUFFIX,
            keysFieldSchema,
            mapField.doc(),
            new ArrayList<String>(),
            mapField.order());

    org.apache.avro.Schema valuesFieldSchema =
        org.apache.avro.Schema.createArray(mapField.schema().getValueType());

    Field valuesField =
        new Field(
            mapField.name() + PINOT_SCHEMA_MAP_VALUES_SUFFIX,
            valuesFieldSchema,
            mapField.doc(),
            new ArrayList<>(),
            mapField.order());

    return new Pair<>(keysField, valuesField);
  }

  public static TableConfig buildPinotTableConfig(
      ViewCreationSpec viewCreationSpec, PinotTableSpec pinotTableSpec, TableType tableType) {
    TableConfigBuilder tableConfigBuilder =
        new TableConfigBuilder(tableType)
            .setTableName(pinotTableSpec.getTableName())
            .setLoadMode(pinotTableSpec.getLoadMode())
            .setSchemaName(viewCreationSpec.getViewName())
            // Indexing related fields
            .setInvertedIndexColumns(pinotTableSpec.getInvertedIndexColumns())
            .setBloomFilterColumns(pinotTableSpec.getBloomFilterColumns())
            .setNoDictionaryColumns(pinotTableSpec.getNoDictionaryColumns())
            .setRangeIndexColumns(pinotTableSpec.getRangeIndexColumns())
            // Segment configs
            .setTimeColumnName(pinotTableSpec.getTimeColumn())
            .setTimeType(pinotTableSpec.getTimeUnit().toString())
            .setNumReplicas(pinotTableSpec.getNumReplicas())
            .setRetentionTimeValue(pinotTableSpec.getRetentionTimeValue())
            .setRetentionTimeUnit(pinotTableSpec.getRetentionTimeUnit())
            .setPeerSegmentDownloadScheme(pinotTableSpec.getPeerSegmentDownloadScheme())
            // Tenant configs
            .setBrokerTenant(pinotTableSpec.getBrokerTenant())
            .setServerTenant(pinotTableSpec.getServerTenant())
            .setTagOverrideConfig(toTagOverrideConfig(pinotTableSpec.getTagOverrideConfigs()))
            // Tier configs
            .setTierConfigList(getTableTierConfigs(pinotTableSpec))
            // Task configurations
            .setTaskConfig(toTableTaskConfig(pinotTableSpec.getTaskConfigs()))
            // ingestion configurations
            .setIngestionConfig(getTableIngestionConfig(pinotTableSpec));

    if (tableType == TableType.REALTIME) {
      // Stream configs only for REALTIME
      tableConfigBuilder
          .setLLC(isLLC(pinotTableSpec.getStreamConfigs()))
          .setStreamConfigs((Map) pinotTableSpec.getStreamConfigs());
    }

    // Unable to set few configs via building hence setting them later directly
    final TableConfig tableConfig = tableConfigBuilder.build();
    tableConfig.getIndexingConfig().setAggregateMetrics(pinotTableSpec.isAggregateMetrics());

    return tableConfig;
  }

  private static IngestionConfig getTableIngestionConfig(@Nullable PinotTableSpec tableSpec) {
    List<Config> transformConfigs = tableSpec.getTransformConfigs();
    List<TransformConfig> tableTransformConfigs = null;
    if (transformConfigs != null) {
      tableTransformConfigs = new ArrayList<>();
      for (Config transformConfig : transformConfigs) {
        tableTransformConfigs.add(
            new TransformConfig(
                transformConfig.getString(PINOT_TRANSFORM_COLUMN_NAME),
                transformConfig.getString(PINOT_TRANSFORM_COLUMN_FUNCTION)));
      }
    }
    return new IngestionConfig(null, null, null, tableTransformConfigs);
  }

  private static TagOverrideConfig toTagOverrideConfig(Config tenantTagOverrideConfig) {
    if (tenantTagOverrideConfig == null) {
      return null;
    }

    String realtimeConsuming =
        getOptionalString(tenantTagOverrideConfig, PINOT_RT_CONSUMING_TAG_KEY, null);
    String realtimeCompleted =
        getOptionalString(tenantTagOverrideConfig, PINOT_RT_COMPLETED_TAG_KEY, null);

    return new TagOverrideConfig(realtimeConsuming, realtimeCompleted);
  }

  private static List<TierConfig> getTableTierConfigs(@Nullable PinotTableSpec tableSpec) {
    List<Config> tierConfigs = tableSpec.getTierConfigs();
    List<TierConfig> tableTierConfigs = null;
    if (tierConfigs != null) {
      tableTierConfigs = new ArrayList<>();
      for (Config tierConfig : tierConfigs) {
        tableTierConfigs.add(
            new TierConfig(
                tierConfig.getString(PINOT_TIER_NAME),
                tierConfig.getString(PINOT_TIER_SEGMENT_SELECTOR_TYPE),
                getOptionalString(tierConfig, PINOT_TIER_SEGMENT_AGE, null),
                tierConfig.getString(PINOT_TIER_STORAGE_TYPE),
                getOptionalString(tierConfig, PINOT_TIER_SERVER_TAG, null)));
      }
    }
    return tableTierConfigs;
  }

  private static TableTaskConfig toTableTaskConfig(@Nullable Config allTasksConfigs) {
    if (allTasksConfigs == null) {
      return null;
    }
    Map<String, Map<String, String>> taskTypeConfigsMap = Maps.newHashMap();
    for (Entry<String, ConfigValue> entry : allTasksConfigs.entrySet()) {
      String taskType = ConfigUtil.splitPath(entry.getKey()).get(0);
      Map<String, String> taskConfig = ConfigUtils.getFlatMapConfig(allTasksConfigs, taskType);
      taskTypeConfigsMap.put(taskType, taskConfig);
    }
    return new TableTaskConfig(taskTypeConfigsMap);
  }

  private static boolean isLLC(Map<String, Object> streamConfigs) {
    return "simple".equals(streamConfigs.get(STREAM_KAFKA_CONSUMER_TYPE_KEY))
        || "LowLevel".equals(streamConfigs.get(STREAM_KAFKA_CONSUMER_TYPE_KEY));
  }

  public static boolean sendPinotTableCreationRequest(
      PinotTableSpec pinotTableSpec, final TableConfig tableConfig) {
    return sendPinotTableCreationRequest(
        pinotTableSpec.getControllerHost(),
        pinotTableSpec.getControllerPort(),
        tableConfig.toJsonString(),
        tableConfig.getTableName());
  }

  /** Utility for sending Pinot Table Creation/Update request. */
  public static boolean sendPinotTableCreationRequest(
      String controllerHost,
      String controllerPort,
      final String tableConfig,
      final String tableName) {
    String controllerAddress = getControllerAddressForTableCreate(controllerHost, controllerPort);
    LOGGER.info("Trying to send table creation request {} to {}. ", tableConfig, controllerAddress);
    int responseCode = sendRequest("POST", controllerAddress, tableConfig);
    /*
       If the response code is HTTP_CONFLICT (409), this means there is already a table in Pinot, hence now we are attempting to update the table.
       Note: Pinot upserts (update/create) REALTIME tables in POST calls however only creates OFFLINE tables in POST calls.
       Hence we need to check for conflict to trigger explicit update for OFFLINE table.
    */
    if (responseCode == HTTP_CONFLICT) {
      LOGGER.info("Trying to update table with request {} to {}. ", tableConfig, controllerAddress);
      sendRequest(
          "PUT",
          getControllerAddressForTableUpdate(controllerHost, controllerPort, tableName),
          tableConfig);
    }
    return true;
  }

  /**
   * @param requestMethod: Type of request (PUT/GET/POST/DELTE)
   * @param urlString: Api endpoint
   * @param payload: Payload
   * @return Status Code
   */
  public static int sendRequest(String requestMethod, String urlString, String payload) {
    HttpURLConnection conn = null;
    try {
      final URL url = new URL(urlString);

      conn = (HttpURLConnection) url.openConnection();
      conn.setDoOutput(true);
      conn.setRequestMethod(requestMethod);

      final BufferedWriter writer =
          new BufferedWriter(
              new OutputStreamWriter(conn.getOutputStream(), StandardCharsets.UTF_8));
      writer.write(payload, 0, payload.length());
      writer.flush();

      final int responseCode = conn.getResponseCode();
      if (responseCode != HTTP_OK) {
        LOGGER.warn(
            "Pinot request failed. Response code: "
                + conn.getResponseCode()
                + " Response: "
                + readInputStream(conn.getErrorStream()));
        return responseCode;
      }
      String successResponse = readInputStream(conn.getInputStream());
      LOGGER.info("Pinot request successful. Response: {}", successResponse);
    } catch (Exception e) {
      String errorResponse = readInputStream(conn.getErrorStream());
      throw new RuntimeException("Error while sending request to pinot: " + errorResponse, e);
    }
    return HTTP_OK;
  }

  /** Utility for reading from an InputStream and converting it to String */
  private static String readInputStream(InputStream inputStream) {
    final StringBuilder sb = new StringBuilder();
    try {
      final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line);
      }
    } catch (Exception e) {
      throw new RuntimeException("Exception while reading the stream", e);
    }

    return sb.toString();
  }

  /**
   * Utility for preparing the api-endpoint for Pinot Table Creation
   *
   * @param controllerHost : hostname
   * @param controllerPort : port
   * @return
   */
  private static String getControllerAddressForTableCreate(
      String controllerHost, String controllerPort) {
    return String.format("http://%s:%s/%s", controllerHost, controllerPort, PINOT_REST_URI_TABLES);
  }

  /**
   * Utility for preparing the api-endpoint for Pinot Table Update
   *
   * @param controllerHost : hostname
   * @param controllerPort : port
   * @param tableName: Name of the table to be updated
   * @return
   */
  private static String getControllerAddressForTableUpdate(
      String controllerHost, String controllerPort, String tableName) {
    return String.format(
        "http://%s:%s/%s/%s", controllerHost, controllerPort, PINOT_REST_URI_TABLES, tableName);
  }

  private static String getOptionalString(Config config, String key, String defaultValue) {
    if (config.hasPath(key)) {
      return config.getString(key);
    }
    return defaultValue;
  }

  private static Config getOptionalConfig(Config config, String key) {
    if (config.hasPath(key)) {
      return config.getConfig(key);
    }
    return ConfigFactory.empty();
  }
}
