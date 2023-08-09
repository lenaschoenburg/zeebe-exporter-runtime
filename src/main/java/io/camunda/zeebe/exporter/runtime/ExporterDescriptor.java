package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.util.ReflectUtil;
import java.util.Map;

public class ExporterDescriptor {
  private final ExporterConfiguration configuration;
  private final Class<? extends Exporter> exporterClass;

  public ExporterDescriptor(
      final String id,
      final Class<? extends Exporter> exporterClass,
      final Map<String, Object> args) {
    this.exporterClass = exporterClass;
    configuration = new ExporterConfiguration(id, args);
  }

  public Exporter newInstance() {
    try {
      return ReflectUtil.newInstance(exporterClass);
    } catch (final Exception e) {
      throw new RuntimeException(getId(), e);
    }
  }

  public ExporterConfiguration getConfiguration() {
    return configuration;
  }

  public String getId() {
    return configuration.id();
  }
}
