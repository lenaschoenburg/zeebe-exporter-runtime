package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.adapter.Adapter;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.runtime.ExporterService;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.camunda.community.eze.EngineFactory;
import org.camunda.community.eze.ZeebeEngine;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class AdapterTest {

  private TestExporter testExporter;
  private ZeebeEngine engine;

  private final class TestExporter implements Exporter {

    public final List<Record> records = new ArrayList<>();

    @Override
    public void export(Record<?> record) {
      records.add(record);
    }
  }

  @BeforeEach
  void setup() throws IOException {
    final var serverName = InProcessServerBuilder.generateName();
    testExporter = new TestExporter();
    final var server =
        InProcessServerBuilder.forName(serverName)
            .addService(new ExporterService(List.of(testExporter)))
            .build()
            .start();
    final var channel = InProcessChannelBuilder.forName(serverName).build();

    engine = EngineFactory.INSTANCE.create(List.of(new Adapter(channel)));
    engine.start();
  }

  @Test
  void testExporterAdapter() throws InterruptedException {
    // given

    // when
    engine
        .createClient()
        .newPublishMessageCommand()
        .messageName("test")
        .correlationKey("test")
        .send()
        .join();

    // then
    Thread.sleep(1_000);
    assert testExporter.records.size() == 2 : "Records: " + testExporter.records;
  }
}
