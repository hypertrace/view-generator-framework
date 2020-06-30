package org.hypertrace.core.viewgenerator;

import org.hypertrace.core.viewgenerator.test.api.SpanTypeOne;
import org.hypertrace.core.viewgenerator.test.api.SpanTypeTwo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class JavaCodeBasedViewGeneratorTest {
  @Test
  public void testJavaCodeBasedViewGenerator() {
    TestViewGenerator testViewGenerator = new TestViewGenerator();

    Assertions.assertEquals(SpanTypeTwo.class.getName(), testViewGenerator.getViewName());
    Assertions.assertEquals(SpanTypeTwo.getClassSchema(), testViewGenerator.getSchema());
    Assertions.assertEquals(SpanTypeTwo.class, testViewGenerator.getViewClass());
    SpanTypeOne span = SpanTypeOne.newBuilder()
        .setSpanId("span-id-1")
        .setStartTimeMillis(10L)
        .setEndTimeMillis(20L)
        .setSpanKind("SERVER")
        .build();

    Assertions.assertEquals(List.of(
        SpanTypeTwo.newBuilder()
            .setSpanId("span-id-1")
            .setStartTimeMillis(10L)
            .setEndTimeMillis(20L)
            .setSpanKind("SERVER")
            .build()),
        testViewGenerator.process(span)
        );
  }
}
