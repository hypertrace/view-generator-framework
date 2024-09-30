package org.hypertrace.core.viewgenerator.api;

import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;

public interface ClientRegistry {
  GrpcChannelRegistry getChannelRegistry();
}
