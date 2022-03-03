package org.hypertrace.core.viewgenerator.service;

import static org.hypertrace.core.viewgenerator.service.ViewGeneratorConstants.*;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.To;
import org.hypertrace.core.viewgenerator.JavaCodeBasedViewGenerator;

public class ViewGenerationProcessTransformer<IN extends SpecificRecord, OUT extends GenericRecord>
    implements Transformer<String, IN, List<KeyValue<String, OUT>>> {

  private final String viewGenName;

  private Config jobConfig;
  private String viewgenClassName;
  private Class<OUT> viewClass;
  private JavaCodeBasedViewGenerator<IN, OUT> viewGenerator;
  private ProcessorContext context;
  private To outputTopic;

  public ViewGenerationProcessTransformer(String viewGenName) {
    this.viewGenName = viewGenName;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
    this.jobConfig = (Config) context.appConfigs().get(this.viewGenName);

    String outputTopicName = jobConfig.getString(OUTPUT_TOPIC_CONFIG_KEY);
    this.outputTopic = To.child(outputTopicName);

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
  public List<KeyValue<String, OUT>> transform(String key, IN value) {
    List<KeyValue<String, OUT>> outputKVPairs = new ArrayList<>();
    List<OUT> output = viewGenerator.process(value);
    if (output != null) {
      for (OUT out : output) {
        outputKVPairs.add(KeyValue.pair(key, out));
      }
    }
    return outputKVPairs;
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
