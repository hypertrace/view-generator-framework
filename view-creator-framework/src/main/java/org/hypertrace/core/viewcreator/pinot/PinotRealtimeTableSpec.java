package org.hypertrace.core.viewcreator.pinot;

import com.typesafe.config.Optional;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import lombok.Data;

@Data
public class PinotRealtimeTableSpec extends PinotTableSpec {
  // Stream configs
  private Map<String, Object> streamConfigs;

  public PinotRealtimeTableSpec() {
  }
}
