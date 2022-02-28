package org.hypertrace.core.viewgenerator;

import com.typesafe.config.Config;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface ViewGenerator<T extends GenericRecord> {

  /** Configure the view generator */
  public default void configure(Config config) {}

  /** Name of the generated View. This should be unique across entire name space. */
  String getViewName();

  /** Avro schema describing the View */
  Schema getSchema();

  /** Generated view class. */
  Class<T> getViewClass();
}
