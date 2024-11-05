package org.hypertrace.core.viewgenerator;

import java.util.List;
import org.apache.avro.Schema;
import org.hypertrace.core.viewgenerator.test.api.RawServiceType;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;

public class TestRawServiceViewGenerator
    implements org.hypertrace.core.viewgenerator.api.ViewGenerator<SpanTypeOne, RawServiceType> {
  @Override
  public List<RawServiceType> process(SpanTypeOne span) {
    return List.of(
        RawServiceType.newBuilder()
            .setSpanId(span.getSpanId())
            .setStartTimeMillis(span.getStartTimeMillis())
            .setEndTimeMillis(span.getEndTimeMillis())
            .setSpanKind(span.getSpanKind())
            .build());
  }

  @Override
  public String getViewName() {
    return RawServiceType.class.getName();
  }

  @Override
  public Schema getSchema() {
    return RawServiceType.getClassSchema();
  }

  @Override
  public Class<RawServiceType> getViewClass() {
    return RawServiceType.class;
  }
}
