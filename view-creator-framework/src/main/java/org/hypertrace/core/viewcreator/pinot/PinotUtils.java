package org.hypertrace.core.viewcreator.pinot;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigBeanFactory;
import com.typesafe.config.ConfigValue;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.pinot.tools.admin.command.AbstractBaseAdminCommand;
import org.apache.pinot.tools.admin.command.AddSchemaCommand;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(PinotUtils.class);
  private static final String MAP_KEYS_SUFFIX = "__KEYS";
  private static final String MAP_VALUES_SUFFIX = "__VALUES";

  public static final String PINOT_CONFIGS_KEY = "pinot";
  public static final String PINOT_STREAM_CONFIGS_KEY = "pinot.streamConfigs";
  public static final String STREAM_KAFKA_DECODER_CLASS_NAME_KEY = "stream.kafka.decoder.class.name";
  public static final String STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY = "stream.kafka.decoder.prop.schema";
  public static final String STREAM_KAFKA_CONSUMER_TYPE_KEY = "stream.kafka.consumer.type";

  public static final String SIMPLE_AVRO_MESSAGE_DECODER = "org.apache.pinot.core.realtime.stream.SimpleAvroMessageDecoder";
  private static final String PINOT_TABLES_CREATE_SUFFIX = "tables";


  public static ViewCreationSpec.PinotTableSpec getPinotTableSpecFromViewGenerationSpec(ViewCreationSpec spec) {
    final ViewCreationSpec.PinotTableSpec pinotTableSpec = ConfigBeanFactory
        .create(spec.getViewGeneratorConfig().getConfig(PINOT_CONFIGS_KEY), ViewCreationSpec.PinotTableSpec.class);
    final Config streamConfigs = spec.getViewGeneratorConfig().getConfig(PINOT_STREAM_CONFIGS_KEY);
    // ConfigBeanFactory will parse Map to nested structure. Try to reset streamConfigs as a flatten map.
    Map<String, Object> streamConfigsMap = new HashMap();
    final Set<Entry<String, ConfigValue>> entries = streamConfigs.entrySet();
    for (Entry<String, ConfigValue> entry : entries) {
      streamConfigsMap.put(entry.getKey(), entry.getValue().unwrapped().toString());
    }
    if (SIMPLE_AVRO_MESSAGE_DECODER
        .equals(streamConfigsMap.get(STREAM_KAFKA_DECODER_CLASS_NAME_KEY))) {
      // Try to infer output schema from ViewCreationSpec if not provided.
      if (!streamConfigsMap.containsKey(STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY)) {
        streamConfigsMap
            .put(STREAM_KAFKA_DECODER_PROP_SCHEMA_KEY, spec.getOutputSchema().toString());
      }
    }
    pinotTableSpec.setStreamConfigs(streamConfigsMap);
    return pinotTableSpec;
  }

  public static Schema createPinotSchemaForView(ViewCreationSpec spec) {
    final ViewCreationSpec.PinotTableSpec pinotTableSpec = getPinotTableSpecFromViewGenerationSpec(spec);
    String schemaName = spec.getViewName();
    org.apache.avro.Schema avroSchema = spec.getOutputSchema();
    TimeUnit timeUnit = pinotTableSpec.getTimeUnit();
    String timeColumn = pinotTableSpec.getTimeColumn();
    List<String> dimensionColumns = pinotTableSpec.getDimensionColumns();
    List<String> metricColumns = pinotTableSpec.getMetricColumns();
    Map<String, Object> columnsMaxLength = pinotTableSpec.getColumnsMaxLength();
    return convertAvroSchemaToPinotSchema(
        avroSchema, schemaName, timeColumn, timeUnit, dimensionColumns, metricColumns, columnsMaxLength);
  }

  public static boolean uploadPinotSchema(ViewCreationSpec spec, final Schema schema) {
    final ViewCreationSpec.PinotTableSpec pinotTableSpec = getPinotTableSpecFromViewGenerationSpec(spec);
    return uploadPinotSchema(pinotTableSpec.getControllerHost(), pinotTableSpec.getControllerPort(),
        schema);
  }

  public static boolean uploadPinotSchema(String controllerHost, String controllerPort,
                                          final Schema pinotSchemaFromAvroSchema) {
    File tmpFile = null;
    try {
      tmpFile = File
          .createTempFile(
              String.format("temp-schema-%s-", pinotSchemaFromAvroSchema.getSchemaName()), ".json");
      LOGGER.info("Created a temp schema file {}", tmpFile.getAbsolutePath());
      BufferedWriter writer = new BufferedWriter(new FileWriter(tmpFile));
      writer.write(pinotSchemaFromAvroSchema.toPrettyJsonString());
      writer.flush();
      return new AddSchemaCommand().setControllerHost(controllerHost)
          .setControllerPort(controllerPort).setSchemaFilePath(tmpFile.getPath())
          .setExecute(true)
          .execute();
    } catch (Exception e) {
      LOGGER.error("Failed to upload Pinot schema.", e);
      return false;
    } finally {
      FileUtils.deleteQuietly(tmpFile);
    }
  }

  public static Schema convertAvroSchemaToPinotSchema(org.apache.avro.Schema avroSchema,
                                                      String name, String timeColumn, TimeUnit timeUnit, List<String> dimensionColumns,
                                                      List<String> metricColumns, Map<String, Object> columnsMaxLength) {
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
        fieldSpec = new DimensionFieldSpec(key.name(),
            AvroUtils.extractFieldDataType(key),
            AvroUtils.isSingleValueField(key));
        // adds the key here first
        fieldSpecs.add(fieldSpec);

        fieldSpec = new DimensionFieldSpec(value.name(),
            AvroUtils.extractFieldDataType(value),
            AvroUtils.isSingleValueField(value));
        fieldSpecs.add(fieldSpec);

      } else if (timeColumn.equals(field.name())) {
        fieldSpec = new TimeFieldSpec(new TimeGranularitySpec(AvroUtils.extractFieldDataType(field), timeUnit, timeColumn));
        fieldSpecs.add(fieldSpec);
      } else if (dimensionColumns.contains(field.name())) {
        fieldSpec = new DimensionFieldSpec(field.name(), AvroUtils.extractFieldDataType(field),
            AvroUtils.isSingleValueField(field));
        fieldSpecs.add(fieldSpec);
      } else if (metricColumns.contains(field.name())) {
        fieldSpec = new MetricFieldSpec(field.name(), AvroUtils.extractFieldDataType(field));
        fieldSpecs.add(fieldSpec);
      }

      for (FieldSpec convertedSpec : fieldSpecs) {
        if (convertedSpec != null) {
          Object defaultVal;
          if (field.schema().getType().equals(Type.MAP)) {
            // we need to set it empty string instead of the default map because it's being split
            // to 2 different string
            defaultVal = "";
          } else {
            defaultVal = field.defaultVal();
          }
          if (defaultVal == JsonProperties.NULL_VALUE) {
            defaultVal = null;
          }
          if (!AvroUtils.isSingleValueField(field)
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

  public static Pair<Field, Field> splitMapField(Field mapField) {
    org.apache.avro.Schema keysFieldSchema =
        org.apache.avro.Schema.createArray(org.apache.avro.Schema.create(Type.STRING));
    Field keysField = new Field(
        mapField.name() + MAP_KEYS_SUFFIX,
        keysFieldSchema,
        mapField.doc(),
        new ArrayList<String>(),
        mapField.order());

    org.apache.avro.Schema valuesFieldSchema =
        org.apache.avro.Schema.createArray(mapField.schema().getValueType());

    Field valuesField = new Field(
        mapField.name() + MAP_VALUES_SUFFIX,
        valuesFieldSchema,
        mapField.doc(),
        new ArrayList<>(),
        mapField.order());

    return new Pair<>(keysField, valuesField);
  }

  public static TableConfig createPinotTableConfig(ViewCreationSpec viewCreationSpec) {
    final ViewCreationSpec.PinotTableSpec pinotTableSpec = getPinotTableSpecFromViewGenerationSpec(
        viewCreationSpec);

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
    final ViewCreationSpec.PinotTableSpec pinotTableSpec = getPinotTableSpecFromViewGenerationSpec(spec);
    return sendPinotTableCreationRequest(pinotTableSpec.getControllerHost(),
        pinotTableSpec.getControllerPort(), tableConfig.toJsonString());
  }

  public static boolean sendPinotTableCreationRequest(String controllerHost,
                                                      String controllerPort, final String tableConfig) {
    try {
      String controllerAddress = getControllerAddressForTableCreate(controllerHost, controllerPort);
      LOGGER.info("Trying to send table creation request {} to {}. ", tableConfig, controllerAddress);
      String res = AbstractBaseAdminCommand.sendPostRequest(controllerAddress, tableConfig);
      LOGGER.info(res);
      return true;
    } catch (Exception e) {
      LOGGER.error("Failed to create Pinot table.", e);
      return false;
    }
  }

  private static String getControllerAddressForTableCreate(String controllerHost, String controllerPort) {
    return String.format("http://%s:%s/%s", controllerHost, controllerPort, PINOT_TABLES_CREATE_SUFFIX);
  }
}