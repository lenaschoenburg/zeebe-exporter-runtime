package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.exporter.api.context.ScheduledTask;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Consumer;

public class ExporterController implements Controller {
  private final Consumer<Long> updatePosition;
  private byte[] metadata;

  public ExporterController(Consumer<Long> updatePosition) {
    this.updatePosition = updatePosition;
  }

  @Override
  public void updateLastExportedRecordPosition(long position) {
    updatePosition.accept(position);
  }

  @Override
  public void updateLastExportedRecordPosition(long position, byte[] metadata) {
    setMetadata(metadata);
    updatePosition.accept(position);
  }

  @Override
  public ScheduledTask scheduleCancellableTask(Duration delay, Runnable task) {
    throw new UnsupportedOperationException("Not implemented yet");
  }

  @Override
  public Optional<byte[]> readMetadata() {
    return Optional.ofNullable(metadata);
  }

  public void setMetadata(byte[] metadata) {
    this.metadata = metadata;
  }
}
