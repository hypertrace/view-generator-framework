package org.hypertrace.core.viewgenerator.service;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FlinkEnvironmentFactoryTest {
  @Test
  public void testCreateExecutionEnv() throws Exception {
    Config metricsConfig = ConfigFactory.parseMap(
        Map.of(
            "metrics.reporters", "prometheus",
            "metrics.reporter.prometheus.class", "org.hypertrace.core.serviceframework.metrics.flink.PrometheusReporter",
            "metrics.reporter.slf4j.class", "org.apache.flink.metrics.slf4j.Slf4jReporter",
            "metrics.reporter.slf4j.interval", "60 SECONDS"
        )
    );
    Config flinkJobConfig = mock(Config.class);
    when(flinkJobConfig.getConfig(ViewGenerationJob.JobConfig.METRICS)).thenReturn(metricsConfig);
    when(flinkJobConfig.hasPath(ViewGenerationJob.JobConfig.PARALLELISM)).thenReturn(true);
    when(flinkJobConfig.getInt(ViewGenerationJob.JobConfig.PARALLELISM)).thenReturn(1);

    Config mainConfig = mock(Config.class);
    when(mainConfig.getConfig(ViewGenerationJob.JobConfig.FLINK_JOB)).thenReturn(flinkJobConfig);

    StreamExecutionEnvironment executionEnvironment = FlinkEnvironmentFactory.createExecutionEnv(mainConfig);
    Assertions.assertNotNull(executionEnvironment);
    Assertions.assertEquals(1, executionEnvironment.getParallelism());
    Assertions.assertEquals(TimeCharacteristic.EventTime, executionEnvironment.getStreamTimeCharacteristic());
    Assertions.assertEquals(UnmodifiableCollectionsSerializer.class,
        executionEnvironment.getConfig().getDefaultKryoSerializerClasses().get(Class.forName("java.util.Collections$UnmodifiableCollection")));
  }
}
