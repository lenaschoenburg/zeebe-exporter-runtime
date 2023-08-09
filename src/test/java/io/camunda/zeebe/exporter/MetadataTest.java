package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.adapter.Adapter;
import io.camunda.zeebe.exporter.runtime.ExporterService;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.camunda.community.eze.EngineFactory;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

final class MetadataTest {

  @Test
  void metadataIsInitializedToEmpty() throws IOException, InterruptedException {
    final var serverName = InProcessServerBuilder.generateName();
    final var testExporter = new AdapterTest.TestExporter();
    final var server =
        InProcessServerBuilder.forName(serverName)
            .addService(new ExporterService(List.of(testExporter)))
            .build()
            .start();
    final var channel = InProcessChannelBuilder.forName(serverName).build();

    final var adapter = new Adapter(channel);
    final var engine = EngineFactory.INSTANCE.create(List.of(adapter));
    engine.start();

    // then
    Assertions.assertThat(adapter.getLastAck().getMetadataMap()).isEmpty();
    // cleanup
    engine.stop();
    channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  void metadataIsUpdated() throws IOException, InterruptedException {
    // given
    final var serverName = InProcessServerBuilder.generateName();
    final var testExporter = new AdapterTest.TestExporter();
    final var server =
        InProcessServerBuilder.forName(serverName)
            .addService(new ExporterService(List.of(testExporter)))
            .build()
            .start();
    final var channel = InProcessChannelBuilder.forName(serverName).build();

    final var adapter = new Adapter(channel);
    final var engine = EngineFactory.INSTANCE.create(List.of(adapter));
    engine.start();

    try (final var client = engine.createClient()) {
      client.newPublishMessageCommand().messageName("test").correlationKey("test").send().join();
    }

    // when
    testExporter.updatePosition(10L, "test".getBytes());

    // then
    Awaitility.await().until(adapter::getPosition, Matchers.equalTo(10L));
    Assertions.assertThat(adapter.getLastAck().getMetadataMap())
        .hasEntrySatisfying(
            AdapterTest.TestExporter.class.getSimpleName(),
            value -> Assertions.assertThat(value.toStringUtf8()).isEqualTo("test"));

    // cleanup
    engine.stop();
    channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
  }
}
