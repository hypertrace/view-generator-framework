package org.hypertrace.core.viewgenerator.service;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;

public class InputToViewMapper<IN extends SpecificRecord, OUT extends GenericRecord>
    implements ValueMapper<IN, List<OUT>> {

  private String viewgenClassName;
  private Class<OUT> viewClass;
  private JavaCodeBasedViewGenerator<IN, OUT> viewGenerator;

  public InputToViewMapper(String viewgenClassName) throws Exception {
    this.viewgenClassName = viewgenClassName;
    this.viewGenerator = createViewGenerator();
    this.viewClass = viewGenerator.getViewClass();
  }

  @Override
  public List<OUT> apply(IN value) {
    return viewGenerator.process(value);
  }

  private JavaCodeBasedViewGenerator createViewGenerator() throws Exception {
    return (JavaCodeBasedViewGenerator)
        Class.forName(this.viewgenClassName).getDeclaredConstructor().newInstance();
  }

  public Class<OUT> getViewClass() {
    return this.viewClass;
  }
}
