package org.hypertrace.core.viewcreator.pinot;

import com.google.common.collect.Maps;
import com.typesafe.config.ConfigBeanFactory;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFieldSpec.TimeFormat;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeFormatUnitSpec;
import org.apache.pinot.spi.data.DateTimeFormatUnitSpec.DateTimeTransformUnit;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.apache.pinot.tools.admin.command.AddSchemaCommand;
import org.hypertrace.core.serviceframework.config.ConfigUtils;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotUtils {

  public static final String PINOT_CONFIGS_KEY = "pinot";
  public static final String PINOT_STREAM_CONFIGS_KEY = "pinot.streamConfigs";
  public static final String STREAM_KAFKA_DECODER_CLASS_NAME_KEY =
      "stream.kafka.decoder.class.name";
  public static final String STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY =
      "stream.kafka.decoder.prop.schema";
  public static final String STREAM_KAFKA_CONSUMER_TYPE_KEY = "stream.kafka.consumer.type";
  public static final String SIMPLE_AVRO_MESSAGE_DECODER = "org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder";
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotUtils.class);
  private static final String MAP_KEYS_SUFFIX = "__KEYS";
  private static final String MAP_VALUES_SUFFIX = "__VALUES";
  private static final String PINOT_TABLES_CREATE_SUFFIX = "tables";


  public static PinotRealtimeTableSpec getPinotRealTimeTableSpec(ViewCreationSpec spec) {
    final PinotRealtimeTableSpec pinotTableSpec = ConfigBeanFactory
        .create(spec.getViewGeneratorConfig().getConfig(PINOT_CONFIGS_KEY),
            PinotRealtimeTableSpec.class);
    // ConfigBeanFactory will parse Map to nested structure. Try to reset streamConfigs as a flatten map.
    Map<String, Object> streamConfigsMap = Maps.newHashMap(
        ConfigUtils.getFlatMapConfig(spec.getViewGeneratorConfig(), PINOT_STREAM_CONFIGS_KEY));
    if (SIMPLE_AVRO_MESSAGE_DECODER
        .equals(streamConfigsMap.get(STREAM_KAFKA_DECODER_CLASS_NAME_KEY))) {

      // Try to infer output schema from ViewCreationSpec if not provided.
      if (!streamConfigsMap.containsKey(STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY)) {
        streamConfigsMap.put(
            STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY, spec.getOutputSchema().toString());
      }
    }
    pinotTableSpec.setStreamConfigs(streamConfigsMap);
    return pinotTableSpec;
  }

  public static Schema createPinotSchemaForView(ViewCreationSpec viewCreationSpec,
      PinotTableSpec pinotTableSpec) {
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
    return uploadPinotSchema(pinotTableSpec.getControllerHost(), pinotTableSpec.getControllerPort(),
        schema);
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
      return new AddSchemaCommand().setControllerHost(controllerHost)
          .setControllerPort(controllerPort).setSchemaFilePath(tmpFile.getPath())
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
        fieldSpec = new DateTimeFieldSpec(field.name(), AvroUtils.extractFieldDataType(field),
            new DateTimeFormatSpec(1, timeUnit.name(),
                TimeFormat.EPOCH.name()).getFormat(),
            new DateTimeGranularitySpec(1, timeUnit).getGranularity());
        fieldSpecs.add(fieldSpec);
      } else if (dateTimeColumns.contains(field.name())) {
        validateDateTimeColumn(field, TimeUnit.MILLISECONDS);
        fieldSpec = new DateTimeFieldSpec(field.name(), AvroUtils.extractFieldDataType(field),
            new DateTimeFormatSpec(1,
                new DateTimeFormatUnitSpec(TimeUnit.MILLISECONDS.toString()).getDateTimeTransformUnit().name(),
                TimeFormat.EPOCH.name()).getFormat(),
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
    // granularity.
    // So, we can support only LONG types and MILLISECOND granularity only
    // Till we fix the schema definition and parsing, these
    final DataType pinotDataType = AvroUtils.extractFieldDataType(field);
    if (pinotDataType != DataType.LONG) {
      throw new RuntimeException(
          "Unsupported type for a date time column. column: " + field.name() + ". Expected: "
              + Type.LONG
              + ", Actual: " + field.schema().getType());
    }
  }

  public static Pair<Field, Field> splitMapField(Field mapField) {
    org.apache.avro.Schema keysFieldSchema =
        org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.STRING));
    Field keysField =
        new Field(
            mapField.name() + MAP_KEYS_SUFFIX,
            keysFieldSchema,
            mapField.doc(),
            new ArrayList<String>(),
            mapField.order());

    org.apache.avro.Schema valuesFieldSchema =
        org.apache.avro.Schema.createArray(mapField.schema().getValueType());

    Field valuesField =
        new Field(
            mapField.name() + MAP_VALUES_SUFFIX,
            valuesFieldSchema,
            mapField.doc(),
            new ArrayList<>(),
            mapField.order());

    return new Pair<>(keysField, valuesField);
  }

  public static TableConfig buildRealTimeTableConfig(ViewCreationSpec viewCreationSpec,
      PinotRealtimeTableSpec pinotTableSpec) {
    return new TableConfigBuilder(TableType.REALTIME)
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
        // Tenant configs
        .setBrokerTenant(pinotTableSpec.getBrokerTenant())
        .setServerTenant(pinotTableSpec.getServerTenant())
        // Stream configs
        .setLLC(isLLC(pinotTableSpec.getStreamConfigs()))
        .setStreamConfigs((Map) pinotTableSpec.getStreamConfigs())
        .build();
  }

  private static boolean isLLC(Map<String, Object> streamConfigs) {
    return "simple".equals(streamConfigs.get(STREAM_KAFKA_CONSUMER_TYPE_KEY))
        || "LowLevel".equals(streamConfigs.get(STREAM_KAFKA_CONSUMER_TYPE_KEY));
  }

  public static boolean sendPinotTableCreationRequest(ViewCreationSpec spec,
      final TableConfig tableConfig) {
    final PinotTableSpec pinotTableSpec = getPinotRealTimeTableSpec(
        spec);
    return sendPinotTableCreationRequest(pinotTableSpec.getControllerHost(),
        pinotTableSpec.getControllerPort(), tableConfig.toJsonString());
  }

  public static boolean sendPinotTableCreationRequest(
      String controllerHost, String controllerPort, final String tableConfig) {
    try {
      String controllerAddress = getControllerAddressForTableCreate(controllerHost, controllerPort);
      LOGGER.info(
          "Trying to send table creation request {} to {}. ", tableConfig, controllerAddress);
      String res = AbstractBaseAdminCommand.sendPostRequest(controllerAddress, tableConfig);
      LOGGER.info(res);
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to create Pinot table.", e);
      return false;
    }
  }

  private static String getControllerAddressForTableCreate(
      String controllerHost, String controllerPort) {
    return String.format(
        "http://%s:%s/%s", controllerHost, controllerPort, PINOT_TABLES_CREATE_SUFFIX);
  }
}
