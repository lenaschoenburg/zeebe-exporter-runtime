package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.record.Record;

import java.util.concurrent.ScheduledExecutorService;

public class ExporterContext {
  private final Exporter exporter;
  private final Runnable positionUpdateNotification;
  private final ExporterController controller;
  private final ScheduledExecutorService executorService;
  private long position;

  public ExporterContext(ScheduledExecutorService executorService, Exporter exporter, Runnable positionUpdateNotification) {
    this.exporter = exporter;
    this.executorService = executorService;
    this.positionUpdateNotification = positionUpdateNotification;
    this.controller = new ExporterController(executorService, this::updatePosition);
  }

  private void updatePosition(Long position) {
    this.position = position;
    positionUpdateNotification.run();
  }

  public long getPosition() {
    return position;
  }

  public void export(Record<?> deserializedRecord) {
    exporter.export(deserializedRecord);
  }

  public void open(byte[] metadata) {
    controller.setMetadata(metadata);
    exporter.open(controller);
  }

  public byte[] getExporterMetadata() {
    return controller.readMetadata().orElse(null);
  }

  public String getId() {
    // TODO: Use actual exporter id
    return exporter.getClass().getSimpleName();
  }
}
