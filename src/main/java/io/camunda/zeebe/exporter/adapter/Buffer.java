package io.camunda.zeebe.exporter.adapter;

import io.camunda.zeebe.protocol.record.Record;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;

final class Buffer {
  private final ConcurrentSkipListMap<Long, Record<?>> records = new ConcurrentSkipListMap<>();

  public Buffer() {}

  void put(final Record<?> record) {
    records.put(record.getPosition(), record);
  }

  void ack(final long position) {
    records.headMap(position, true).clear();
  }

  public void drain(final Consumer<Record<?>> consumer) {
    Map.Entry<Long, Record<?>> entry;
    while ((entry = records.pollFirstEntry()) != null) {
      consumer.accept(entry.getValue());
    }
  }
}
