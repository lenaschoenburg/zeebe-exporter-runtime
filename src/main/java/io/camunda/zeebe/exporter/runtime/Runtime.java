package io.camunda.zeebe.exporter.runtime;

import io.grpc.ServerBuilder;
import java.io.IOException;

public class Runtime {
  public static void main(String[] args) throws InterruptedException, IOException {
    final var server = ServerBuilder.forPort(8080).addService(new ExporterService()).build();
    server.start();
    server.awaitTermination();
  }
}
