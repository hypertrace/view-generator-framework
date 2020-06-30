package org.hypertrace.core.viewgenerator.service;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;

import java.util.List;

/*
 *  The Processor function to generate a View.
 */
public class ViewGenerationProcessFunction<IN extends SpecificRecord, OUT extends GenericRecord> extends ProcessFunction<IN, OUT> {

  private final String viewgenClassName;
  private final Class<OUT> viewClass;
  private transient JavaCodeBasedViewGenerator<IN, OUT> viewGenerator;

  public ViewGenerationProcessFunction(String className) throws Exception {
    this.viewgenClassName = className;
    this.viewClass = (createViewGenerator()).getViewClass();
  }

  // Flink lifecycle method to initialize the function before use
  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.viewGenerator = createViewGenerator();
  }

  private JavaCodeBasedViewGenerator createViewGenerator() throws Exception {
    return (JavaCodeBasedViewGenerator) Class.forName(this.viewgenClassName)
        .getDeclaredConstructor().newInstance();
  }

  public Class<OUT> getViewClass() {
    return this.viewClass;
  }

  @Override
  public void processElement(IN trace, Context ctx, Collector<OUT> out) {
    List<OUT> records = viewGenerator.process(trace);
    if (records != null) {
      for (OUT r : records) {
        out.collect(r);
      }
    }
  }
}

