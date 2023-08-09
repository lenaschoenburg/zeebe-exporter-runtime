package io.camunda.zeebe.exporter.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import io.camunda.zeebe.exporter.ExporterGrpc;
import io.camunda.zeebe.exporter.ExporterOuterClass;
import io.camunda.zeebe.exporter.ExporterOuterClass.ExporterAcknowledgment;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.jackson.ZeebeProtocolModule;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;

public final class ExporterService extends ExporterGrpc.ExporterImplBase {

  private final ObjectMapper mapper;
  private final Map<String, ExporterContext> contexts;
  private final ScheduledExecutorService executorService;
  private StreamObserver<ExporterAcknowledgment> responseObserver;

  public ExporterService(List<Exporter> exporters) {
    mapper = new ObjectMapper().registerModule(new ZeebeProtocolModule());
    executorService = Executors.newSingleThreadScheduledExecutor();
    contexts =
        exporters.stream()
            .map(exporter -> new ExporterContext(executorService, exporter, this::updateExporterPositionAndMetadata))
            .collect(Collectors.toMap(ExporterContext::getId, Function.identity()));
  }

  public void updateExporterPositionAndMetadata() {
    final var newPosition =
        contexts.values().stream().mapToLong(ExporterContext::getPosition).min().orElseThrow();
    final var metadata =
        contexts.values().stream()
            .collect(
                Collectors.toMap(
                    ExporterContext::getId,
                    (context) -> ByteString.copyFrom(context.getExporterMetadata())));

    responseObserver.onNext(
        ExporterAcknowledgment.newBuilder()
            .setPosition(newPosition)
            .putAllMetadata(metadata)
            .build());
  }

  @Override
  public void open(
      ExporterAcknowledgment lastAck,
      StreamObserver<ExporterOuterClass.OpenResponse> responseObserver) {
    final var metadata = lastAck.getMetadataMap();

    for (final var entry : metadata.entrySet()) {
      final var exporterId = entry.getKey();
      final var exporterMetadata = entry.getValue().toByteArray();
      final var context = contexts.get(exporterId);
      context.open(exporterMetadata);
    }
    for (final var context : contexts.values()) {
      context.open(metadata.getOrDefault(context.getId(), ByteString.EMPTY).toByteArray());
    }
  }

  @Override
  public StreamObserver<ExporterOuterClass.Record> exportStream(
      StreamObserver<ExporterAcknowledgment> responseObserver) {
    this.responseObserver = responseObserver;

    return new StreamObserver<>() {
      @Override
      public void onNext(ExporterOuterClass.Record record) {
        try {
          final Record<?> deserializedRecord =
              mapper.readValue(
                  record.getSerialized().toByteArray(), new TypeReference<Record<?>>() {});

          // todo: do we need to have an object for each exporter?
          contexts.values().forEach(context -> context.export(deserializedRecord));
        } catch (IOException e) {
          // todo: what to do ?

          throw new RuntimeException(e);
        }
      }

      @Override
      public void onError(Throwable throwable) {
        System.out.println("Error: " + throwable.getMessage());
      }

      @Override
      public void onCompleted() {
        System.out.println("Stream completed");
      }
    };
  }
}
