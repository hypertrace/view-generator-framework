package org.hypertrace.core.viewgenerator.service;

import java.time.Instant;
import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.hypertrace.core.viewgenerator.api.ViewGenerator;

public class ViewGenerationProcessor<IN extends SpecificRecord, OUT extends GenericRecord>
    implements Processor<String, IN, String, OUT> {
  private ViewGenerator<IN, OUT> viewGenerator;
  private ProcessorContext<String, OUT> context;

  public ViewGenerationProcessor(ViewGenerator<IN, OUT> viewGenerator) {
    this.viewGenerator = viewGenerator;
  }

  @Override
  public void init(ProcessorContext<String, OUT> context) {
    this.context = context;
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
}
