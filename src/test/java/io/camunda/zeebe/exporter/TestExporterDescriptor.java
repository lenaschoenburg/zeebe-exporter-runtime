package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.runtime.ExporterDescriptor;
import java.util.Map;

public class TestExporterDescriptor extends ExporterDescriptor {
  private final Exporter exporter;

  public TestExporterDescriptor(Exporter exporter) {
    super(exporter.getClass().getSimpleName(), exporter.getClass(), Map.of());
    this.exporter = exporter;
  }

  @Override
  public Exporter newInstance() {
    return exporter;
  }
}
