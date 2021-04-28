package org.hypertrace.core.viewcreator.pinot;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.Lists;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.List;
import org.hypertrace.core.viewcreator.ViewCreationSpec;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PinotTableCreationToolTest {

  private HttpServer httpServer;
  private InetSocketAddress socket;
  private PinotTableCreationTool tool;
  private List<String> requests;

  @BeforeEach
  public void setUp() throws IOException {
    // random available port to listen on
    socket = new InetSocketAddress(0);
    httpServer = HttpServer.create(socket, 0);
    httpServer.createContext("/schemas", createHandler(200, "{}"));
    httpServer.createContext("/tables", createHandler(200, "{}"));
    httpServer.start();

    // Overriding the port
    Config configs =
        createConfig("sample-view-generation-spec.conf")
            .withValue(
                "pinot.controllerPort",
                ConfigValueFactory.fromAnyRef(httpServer.getAddress().getPort()));
    ViewCreationSpec viewCreatorSpec = ViewCreationSpec.parse(configs);
    tool = new PinotTableCreationTool(viewCreatorSpec);
    requests = Lists.newArrayList();
  }

  @AfterEach
  public void teardown() throws IOException {
    httpServer.stop(1);
  }

  @Test
  public void testCreate() throws Exception {
    tool.create();
    assertEquals(3, requests.size());
    assertEquals(List.of("/schemas", "/tables", "/tables"), requests);
  }

  private Config createConfig(String resourcePath) {
    File configFile =
        new File(this.getClass().getClassLoader().getResource(resourcePath).getPath());
    return ConfigFactory.parseFile(configFile);
  }

  private HttpHandler createHandler(int status, String response) {
    return exchange -> {
      requests.add(exchange.getRequestURI().getPath());
      exchange.sendResponseHeaders(status, response.length());
      OutputStream responseBody = exchange.getResponseBody();
      responseBody.write(response.getBytes());
      responseBody.close();
    };
  }
}
