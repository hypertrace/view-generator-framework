package org.hypertrace.core.viewgenerator.service;

import com.typesafe.config.Config;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.hypertrace.core.flinkutils.utils.FlinkUtils;
import org.hypertrace.core.serviceframework.config.ConfigUtils;

import java.util.Map;

public class FlinkEnvironmentFactory {

  public static synchronized StreamExecutionEnvironment createExecutionEnv(Config configs) throws Exception {
    Config flinkJobConfig = configs.getConfig(ViewGenerationJob.JobConfig.FLINK_JOB);
    Map<String, String> metricsConfig = ConfigUtils.getFlatMapConfig(flinkJobConfig, ViewGenerationJob.JobConfig.METRICS);
    int parallelism = ConfigUtils.getIntConfig(flinkJobConfig, ViewGenerationJob.JobConfig.PARALLELISM, Runtime.getRuntime().availableProcessors());

    StreamExecutionEnvironment environment = FlinkUtils.getExecutionEnvironment(metricsConfig);
    environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    environment.getConfig()
        .addDefaultKryoSerializer(Class.forName("java.util.Collections$UnmodifiableCollection"),
            UnmodifiableCollectionsSerializer.class);
    environment.setParallelism(parallelism);

    return environment;
  }
}
