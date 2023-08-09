package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.adapter.Adapter;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.exporter.runtime.ExporterService;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.camunda.community.eze.EngineFactory;
import org.camunda.community.eze.ZeebeEngine;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class AdapterTest {

  private TestExporter testExporter;
  private ZeebeEngine engine;
  private Server server;
  private ManagedChannel channel;
  private Adapter adapter;

  static final class TestExporter implements Exporter {

    public final List<Record<?>> records = new ArrayList<>();
    private Controller controller;

    @Override
    public void open(Controller controller) {
      this.controller = controller;
    }

    @Override
    public void export(Record<?> record) {
      records.add(record);
    }

    public void updatePosition(long position) {
      controller.updateLastExportedRecordPosition(position);
    }

    public void updatePosition(long position, byte[] metadata) {
      controller.updateLastExportedRecordPosition(position, metadata);
    }
  }

  @BeforeEach
  void setup() throws IOException {
    final var serverName = InProcessServerBuilder.generateName();
    testExporter = new TestExporter();
    server =
        InProcessServerBuilder.forName(serverName)
            .addService(new ExporterService(List.of(new TestExporterDescriptor(testExporter))))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).build();

    adapter = new Adapter(channel);
    engine = EngineFactory.INSTANCE.create(List.of(adapter));
    engine.start();
  }

  @AfterEach
  void teardown() throws InterruptedException {
    engine.stop();
    channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  void testExporterAdapter() throws InterruptedException {
    // given

    // when
    try (final var client = engine.createClient()) {
      client.newPublishMessageCommand().messageName("test").correlationKey("test").send().join();
    }

    // then
    Awaitility.await().until(() -> testExporter.records.size() == 2);
  }

  @Test
  void positionsAreUpdated() {
    // given
    try (final var client = engine.createClient()) {
      client.newPublishMessageCommand().messageName("test").correlationKey("test").send().join();
    }

    // when
    testExporter.updatePosition(10L);

    // then
    Awaitility.await().until(() -> adapter.getPosition() == 10);
  }

  @Test
  void metadataIsUpdated() {
    // given
    try (final var client = engine.createClient()) {
      client.newPublishMessageCommand().messageName("test").correlationKey("test").send().join();
    }

    // when
    testExporter.updatePosition(10L, "test".getBytes());

    // then
    Awaitility.await().until(adapter::getPosition, Matchers.equalTo(10L));
    Assertions.assertThat(adapter.getLastAck().getMetadataMap())
        .hasEntrySatisfying(
            TestExporter.class.getSimpleName(),
            value -> Assertions.assertThat(value.toStringUtf8()).isEqualTo("test"));
  }
}
