package org.hypertrace.core.viewgenerator.api;

import com.typesafe.config.Config;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.processor.StreamPartitioner;

public interface ViewGenerator<IN extends SpecificRecord, OUT extends GenericRecord> {

  /** Configure the view generator */
  default void configure(Config config, ClientRegistry clientRegistry) {}

  /**
   * List of GenericRecord's. Each Record should conform to getSchema() and will be inserted into
   * output sink's (Kafka)
   */
  List<OUT> process(IN trace);

  /**
   * View generators can choose to implement and customize the output partitioner as needed.
   *
   * @return the partitioner specific to this view. Can return null which means output is random
   *     partitioning.
   */
  default StreamPartitioner<String, OUT> getPartitioner() {
    return null;
  }

  /** Name of the generated View. This should be unique across entire namespace. */
  String getViewName();

  /** Avro schema describing the view */
  Schema getSchema();

  /** Generated view class. */
  Class<OUT> getViewClass();
}
