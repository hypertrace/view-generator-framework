package org.hypertrace.core.viewcreator;

import com.typesafe.config.Config;
import com.typesafe.config.Optional;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecord;

public class ViewCreationSpec {
  private static final String VIEW_NAME_CONFIG = "view.name";
  private static final String VIEW_OUTPUT_SCHEMA_CLASS_CONFIG = "view.output.schema.class";

  private final String viewName;
  private final String viewOutputSchemaClassName;
  private final Config viewGeneratorConfig;

  public ViewCreationSpec(String viewName, String viewOutputSchemaClassName,
                          Config viewGeneratorConfig) {
    this.viewName = viewName;
    this.viewOutputSchemaClassName = viewOutputSchemaClassName;
    this.viewGeneratorConfig = viewGeneratorConfig;
  }

  public static ViewCreationSpec parse(Config configs) {
    return new ViewCreationSpec(
        configs.getString(VIEW_NAME_CONFIG),
        configs.getString(VIEW_OUTPUT_SCHEMA_CLASS_CONFIG),
        configs
    );
  }

  public String getViewName() {
    return viewName;
  }

  public Config getViewGeneratorConfig() {
    return viewGeneratorConfig;
  }

  // Best effort to extract output schema of a JAVA_CODE based ViewGenerator.
  // This method requires a public constructor and getSchema() method is required to be
  // implemented without other runtime dependencies.
  public Schema getOutputSchema() {
    try {
      final Class<SpecificRecord> outputSchemaClass = (Class<SpecificRecord>) Class
          .forName(this.viewOutputSchemaClassName);
      final SpecificRecord schemaObject = outputSchemaClass.getDeclaredConstructor().newInstance();
      return schemaObject.getSchema();
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to get schema from ViewOutputSchemaClass: " + this.viewOutputSchemaClassName, e);
    }
  }
}
