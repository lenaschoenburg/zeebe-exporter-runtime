package io.camunda.zeebe.exporter.adapter;

import com.google.protobuf.ByteString;
import io.camunda.zeebe.exporter.ExporterGrpc;
import io.camunda.zeebe.exporter.ExporterOuterClass;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.impl.record.CopiedRecord;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.agrona.concurrent.UnsafeBuffer;

public final class Adapter implements Exporter {
  private ExporterGrpc.ExporterStub client;
  private ManagedChannel channel;
  private StreamObserver<ExporterOuterClass.Record> requests;
  private ResponseObserver responses;
  private Controller controller;

  public Adapter(ManagedChannel channel) {
    this.channel = channel;
  }

  public Adapter() {}

  @Override
  public void configure(Context context) {
    if (channel == null) {
      channel = ManagedChannelBuilder.forAddress("localhost", 8080).usePlaintext().build();
    }
    client = ExporterGrpc.newStub(channel);
  }

  @Override
  public void close() {
    channel.shutdown();
    requests.onCompleted();
    responses.onCompleted();
  }

  @Override
  public void open(Controller controller) {
    this.controller = controller;
    responses = new ResponseObserver();
    requests = client.export(responses);
  }

  @Override
  public void export(Record<?> record) {
    final var r =
        ExporterOuterClass.Record.newBuilder()
            .setSerialized(ByteString.copyFromUtf8(record.toJson()))
            .build();
    requests.onNext(r);
  }

  private final class ResponseObserver
      implements StreamObserver<ExporterOuterClass.ExporterAcknowledgment> {
    @Override
    public void onNext(ExporterOuterClass.ExporterAcknowledgment value) {
      controller.updateLastExportedRecordPosition(value.getPosition());
    }

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onCompleted() {}
  }
}
