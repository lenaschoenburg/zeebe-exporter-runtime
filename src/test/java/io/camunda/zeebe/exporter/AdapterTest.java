package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.adapter.Adapter;
import io.camunda.zeebe.exporter.runtime.ExporterService;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.List;
import org.camunda.community.eze.EngineFactory;
import org.junit.jupiter.api.Test;

final class AdapterTest {
  @Test
  void testExporterAdapter() throws IOException, InterruptedException {
    final var serverName = InProcessServerBuilder.generateName();
    final var server =
        InProcessServerBuilder.forName(serverName)
            .addService(new ExporterService())
            .build()
            .start();
    final var channel = InProcessChannelBuilder.forName(serverName).build();

    final var engine = EngineFactory.INSTANCE.create(List.of(new Adapter(channel)));
    engine.start();

    engine
        .createClient()
        .newPublishMessageCommand()
        .messageName("test")
        .correlationKey("test")
        .send()
        .join();

    Thread.currentThread().join();
  }
}
