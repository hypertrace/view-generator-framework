package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.OUTPUT_TOPIC_CONFIG_KEY;
import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.VIEW_GENERATOR_CLASS_CONFIG_KEY;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;

public class ViewGenerationProcessTransformer<IN extends SpecificRecord, OUT extends GenericRecord> implements
    Transformer<String, IN, OUT> {

  private String viewgenClassName;
  private Class<OUT> viewClass;
  private JavaCodeBasedViewGenerator<IN, OUT> viewGenerator;
  private ProcessorContext context;
  private To outputTopic;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    String outputTopicName = (String) context.appConfigs().get(OUTPUT_TOPIC_CONFIG_KEY);
    outputTopic = To.child(outputTopicName);
    this.viewgenClassName = (String) context.appConfigs().get(VIEW_GENERATOR_CLASS_CONFIG_KEY);
    try {
      viewGenerator = createViewGenerator();
      viewClass = viewGenerator.getViewClass();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public OUT transform(String key, IN value) {
    List<OUT> output = viewGenerator.process(value);
    if (output != null) {
      for (OUT out : output) {
        context.forward(null, out, outputTopic);
      }
    }
    return null;
  }

  @Override
  public void close() {
  }

  public Class<OUT> getViewClass() {
    return this.viewClass;
  }

  private JavaCodeBasedViewGenerator createViewGenerator() throws Exception {
    return (JavaCodeBasedViewGenerator) Class.forName(this.viewgenClassName)
        .getDeclaredConstructor().newInstance();
  }
}
