package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.*;

import com.typesafe.config.Config;
import java.time.Instant;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;

public class ViewGenerationProcessor<IN extends SpecificRecord, OUT extends GenericRecord>
    implements Processor<String, IN, String, OUT> {

  private final String viewGenName;

  private Config jobConfig;
  private String viewgenClassName;
  private Class<OUT> viewClass;
  private JavaCodeBasedViewGenerator<IN, OUT> viewGenerator;
  private ProcessorContext<String, OUT> context;

  public ViewGenerationProcessor(String viewGenName) {
    this.viewGenName = viewGenName;
  }

  @Override
  public void init(ProcessorContext<String, OUT> context) {
    this.context = context;
    this.jobConfig = (Config) context.appConfigs().get(this.viewGenName);

    this.viewgenClassName = jobConfig.getString(VIEW_GENERATOR_CLASS_CONFIG_KEY);
    try {
      viewGenerator = createViewGenerator();
      viewGenerator.configure(jobConfig);
      viewClass = viewGenerator.getViewClass();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void process(Record<String, IN> record) {
    List<OUT> output = viewGenerator.process(record.value());
    if (output != null) {
      long nowMillis = Instant.now().toEpochMilli();
      for (OUT out : output) {
        context.forward(new Record<>(record.key(), out, nowMillis));
      }
    }
  }

  @Override
  public void close() {}

  public Class<OUT> getViewClass() {
    return this.viewClass;
  }

  private JavaCodeBasedViewGenerator createViewGenerator() throws Exception {
    return (JavaCodeBasedViewGenerator)
        Class.forName(this.viewgenClassName).getDeclaredConstructor().newInstance();
  }
}
