package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.api.Exporter;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.*;

public class Runtime {
  public static void main(String[] args)
      throws InterruptedException, IOException, ClassNotFoundException {
    final Map<String, Object> configuration = parseConfiguration(System.getenv());
    final var descriptors = buildExporterDescriptors(configuration);
    final var server =
        ServerBuilder.forPort(8080).addService(new ExporterService(descriptors)).build();
    server.start();
    server.awaitTermination();
  }

  private static LinkedList<ExporterDescriptor> buildExporterDescriptors(
      Map<String, Object> configuration) throws ClassNotFoundException {
    final var descriptors = new LinkedList<ExporterDescriptor>();
    for (final var exporter : configuration.entrySet()) {
      final var exporterName = exporter.getKey();
      final var exporterConfig = (Map<String, Object>) exporter.getValue();
      final var exporterClass =
          Runtime.class
              .getClassLoader()
              .loadClass((String) exporterConfig.get("classname"))
              .asSubclass(Exporter.class);
      final var exporterArgs = (Map<String, Object>) exporterConfig.get("args");
      final var descriptor = new ExporterDescriptor(exporterName, exporterClass, exporterArgs);
      descriptors.add(descriptor);
    }
    return descriptors;
  }

  public static Map<String, Object> parseConfiguration(Map<String, String> env) {
    final var configuration = new HashMap<String, Object>();
    for (final var key : env.keySet()) {
      final var prefix = "ZEEBE_BROKER_EXPORTERS_";
      if (key.startsWith(prefix)) {
        final var value = env.get(key);
        final var keyParts = key.substring(prefix.length()).split("_");
        final var path = Arrays.copyOf(keyParts, keyParts.length - 1);
        final var name = keyParts[keyParts.length - 1];

        var current = configuration;

        for (final var elem : path) {
          final var existing =
              current.computeIfAbsent(elem.toLowerCase(), k -> new HashMap<String, Object>());
          if (existing instanceof Map<?, ?> existingMap) {
            current = (HashMap<String, Object>) existingMap;
          } else {
            throw new RuntimeException("Invalid configuration");
          }
        }
        current.put(name.toLowerCase(), value);
      }
    }
    return configuration;
  }
}
