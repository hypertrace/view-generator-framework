package org.hypertrace.core.viewgenerator;

import org.apache.avro.Schema;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeTwo;

import java.util.List;

public class TestViewGenerator implements JavaCodeBasedViewGenerator<SpanTypeOne, SpanTypeTwo> {
  @Override
  public List<SpanTypeTwo> process(SpanTypeOne span) {
    return List.of(SpanTypeTwo.newBuilder()
        .setSpanId(span.getSpanId())
        .setStartTimeMillis(span.getStartTimeMillis())
        .setEndTimeMillis(span.getEndTimeMillis())
        .setSpanKind(span.getSpanKind())
        .build());
  }

  @Override
  public String getViewName() {
    return SpanTypeTwo.class.getName();
  }

  @Override
  public Schema getSchema() {
    return SpanTypeTwo.getClassSchema();
  }

  @Override
  public Class<SpanTypeTwo> getViewClass() {
    return SpanTypeTwo.class;
  }
}
