package org.hypertrace.core.viewcreator.pinot;

import java.util.Map;
import lombok.Data;

@Data
public class PinotRealtimeTableSpec extends PinotTableSpec {
  // Stream configs
  private Map<String, Object> streamConfigs;

  public PinotRealtimeTableSpec() {}
}
