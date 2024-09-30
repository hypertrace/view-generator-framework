package org.hypertrace.core.viewgenerator.api;

import com.google.common.base.Preconditions;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public class DefaultClientRegistry implements ClientRegistry {
  private final GrpcChannelRegistry channelRegistry;

  public DefaultClientRegistry(GrpcChannelRegistry channelRegistry) {
    Preconditions.checkNotNull(channelRegistry, "null channel registry");
    this.channelRegistry = channelRegistry;
  }

  @Override
  public GrpcChannelRegistry getChannelRegistry() {
    return channelRegistry;
  }
}
