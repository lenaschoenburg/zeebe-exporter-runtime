package io.camunda.zeebe.exporter.runtime;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.camunda.zeebe.exporter.ExporterGrpc;
import io.camunda.zeebe.exporter.ExporterOuterClass;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.protocol.jackson.ZeebeProtocolModule;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;

public final class ExporterService extends ExporterGrpc.ExporterImplBase {

  private final ObjectMapper mapper;
  private final List<Exporter> exporters;

  public ExporterService(List<Exporter> exporters) {
    mapper = new ObjectMapper().registerModule(new ZeebeProtocolModule());
    this.exporters = exporters;
  }

  @Override
  public StreamObserver<ExporterOuterClass.Record> export(
      StreamObserver<ExporterOuterClass.ExporterAcknowledgment> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(ExporterOuterClass.Record record) {
        try {
          final Record<?> deserializedRecord =
                  mapper.readValue(record.getSerialized().toByteArray(), new TypeReference<Record<?>>() {});

          // todo: do we need to have an object for each exporter?
          exporters.forEach(exporter -> exporter.export(deserializedRecord));

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
