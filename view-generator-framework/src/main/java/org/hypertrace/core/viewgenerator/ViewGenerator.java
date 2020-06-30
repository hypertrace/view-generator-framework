package org.hypertrace.core.viewgenerator;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

public interface ViewGenerator<T extends GenericRecord> {
  /**
   * Name of the generated View. This should be unique across entire name space.
   */
  String getViewName();

  /**
   * Avro schema describing the View
   */
  Schema getSchema();

  /**
   * Generated view class.
   */
  Class<T> getViewClass();
}
