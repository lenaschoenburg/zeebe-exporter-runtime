package io.camunda.zeebe.exporter.runtime;

import io.camunda.zeebe.exporter.ExporterGrpc;
import io.camunda.zeebe.exporter.ExporterOuterClass;
import io.grpc.stub.StreamObserver;

public final class ExporterService extends ExporterGrpc.ExporterImplBase {
  @Override
  public StreamObserver<ExporterOuterClass.Record> export(
      StreamObserver<ExporterOuterClass.ExporterAcknowledgment> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(ExporterOuterClass.Record record) {
        System.out.println("Received record: " + record);
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
