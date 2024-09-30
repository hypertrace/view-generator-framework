package org.hypertrace.core.viewgenerator;

import java.util.List;
import org.apache.avro.Schema;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeTwo;

public class TestSpanEventViewGenerator
    implements org.hypertrace.core.viewgenerator.api.ViewGenerator<SpanTypeOne, SpanTypeTwo> {
  @Override
  public List<SpanTypeTwo> process(SpanTypeOne span) {
    return List.of(
        SpanTypeTwo.newBuilder()
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
