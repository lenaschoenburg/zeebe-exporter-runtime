package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.exporter.api.context.ScheduledTask;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ExporterController implements Controller {
  private final Consumer<Long> updatePosition;
  private final ScheduledExecutorService executorService;
  private byte[] metadata;

  public ExporterController(ScheduledExecutorService executorService, Consumer<Long> updatePosition) {
    this.executorService = executorService;
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
    final var scheduledFuture = executorService.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);

    return () -> scheduledFuture.cancel(true);
  }

  @Override
  public Optional<byte[]> readMetadata() {
    return Optional.ofNullable(metadata);
  }

  public void setMetadata(byte[] metadata) {
    this.metadata = metadata;
  }
}
