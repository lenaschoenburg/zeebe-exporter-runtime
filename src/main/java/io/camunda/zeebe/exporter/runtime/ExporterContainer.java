package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Configuration;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.exporter.api.context.ScheduledTask;
import io.camunda.zeebe.protocol.record.Record;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExporterContainer implements Controller, Context {
  private static final Logger LOG = LoggerFactory.getLogger(ExporterContainer.class);
  private final Exporter exporter;
  private final Runnable positionUpdateNotification;
  private final ScheduledExecutorService executorService;
  private final ExporterConfiguration configuration;
  private long position;
  private long lastUnacknowledgedPosition;

  private byte[] metadata;
  private RecordFilter filter;

  public ExporterContainer(
      ScheduledExecutorService executorService,
      Runnable positionUpdateNotification,
      ExporterDescriptor descriptor) {
    this.executorService = executorService;
    this.positionUpdateNotification = positionUpdateNotification;

    this.configuration = descriptor.getConfiguration();
    this.exporter = descriptor.newInstance();
    try {
      exporter.configure(this);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public long getPosition() {
    return position;
  }

  public void export(Record<?> record) {
    try {
      if (position < record.getPosition()) {
        if (acceptRecord(record)) {
          exporter.export(record);
          lastUnacknowledgedPosition = record.getPosition();
        } else {
          updatePositionOnSkipIfUpToDate(record.getPosition());
        }
      }
    } catch (final Exception ex) {
      LOG.warn("Error on exporting record with key {}", record.getKey(), ex);
    }
  }

  /**
   * Updates the exporter's position if it is up-to-date - that is, if it's last acknowledged
   * position is greater than or equal to its last unacknowledged position. This is safe to do when
   * skipping records as it means we passed no record to this exporter between both.
   *
   * @param eventPosition the new, up-to-date position
   */
  void updatePositionOnSkipIfUpToDate(final long eventPosition) {
    if (position >= lastUnacknowledgedPosition && position < eventPosition) {
      updateLastExportedRecordPosition(eventPosition);
    }
  }

  private boolean acceptRecord(final Record<?> metadata) {
    return filter == null
        || (filter.acceptType(metadata.getRecordType())
            && filter.acceptValue(metadata.getValueType()));
  }

  public void open(byte[] metadata) {
    this.metadata = metadata;
    exporter.open(this);
  }

  public String getId() {
    return configuration.id();
  }

  @Override
  public void updateLastExportedRecordPosition(long position) {
    this.position = position;
    positionUpdateNotification.run();
  }

  @Override
  public void updateLastExportedRecordPosition(long position, byte[] metadata) {
    this.position = position;
    this.metadata = metadata;
    positionUpdateNotification.run();
  }

  @Override
  public ScheduledTask scheduleCancellableTask(Duration delay, Runnable task) {
    final var scheduledFuture =
        executorService.schedule(task, delay.toMillis(), TimeUnit.MILLISECONDS);
    return () -> scheduledFuture.cancel(true);
  }

  @Override
  public Optional<byte[]> readMetadata() {
    return Optional.ofNullable(metadata);
  }

  @Override
  public Logger getLogger() {
    return LOG;
  }

  @Override
  public Configuration getConfiguration() {
    return configuration;
  }

  @Override
  public void setFilter(RecordFilter filter) {
    this.filter = filter;
  }

  public void close() {
    exporter.close();
  }
}
