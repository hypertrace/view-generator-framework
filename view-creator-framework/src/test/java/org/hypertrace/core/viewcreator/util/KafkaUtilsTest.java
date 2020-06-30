package org.hypertrace.core.viewcreator.util;

import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class KafkaUtilsTest {
  @Test
  public void testKafkaSpecParser() {
    ViewCreationSpec viewCreationSpec = ViewCreationSpec.parse(ConfigFactory.parseFile(
        new File(KafkaUtilsTest.class.getClassLoader()
            .getResource("sample-view-generation-spec.conf").getPath())));
    // Test KafkaSpec
    final ViewCreationSpec.KafkaSpec kafkaSpec = KafkaUtils.getKafkaSpecFromViewGenerationSpec(viewCreationSpec);
    Assertions.assertEquals(kafkaSpec.getBrokerAddress(), "localhost:9092");
    Assertions.assertEquals(kafkaSpec.getTopicName(), "test-view-events");
    Assertions.assertEquals(kafkaSpec.getPartitions(), 8);
    Assertions.assertEquals(kafkaSpec.getReplicationFactor(), 3);
  }
}
