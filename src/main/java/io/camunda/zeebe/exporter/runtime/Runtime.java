package io.camunda.zeebe.exporter.runtime;

import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.List;

public class Runtime {
  public static void main(String[] args) throws InterruptedException, IOException {
    final var server =
        ServerBuilder.forPort(8080).addService(new ExporterService(List.of())).build();
    server.start();
    server.awaitTermination();
  }
}
