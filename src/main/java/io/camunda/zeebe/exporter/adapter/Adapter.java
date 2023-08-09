package io.camunda.zeebe.exporter.adapter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.camunda.zeebe.exporter.ExporterGrpc;
import io.camunda.zeebe.exporter.ExporterOuterClass;
import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

public final class Adapter implements Exporter {
  private ExporterGrpc.ExporterStub client;
  private ManagedChannel channel;
  private StreamObserver<ExporterOuterClass.Record> requests;
  private ResponseObserver responses;
  private Controller controller;
  private ExporterOuterClass.ExporterAcknowledgment lastAck;

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
    this.lastAck =
        controller
            .readMetadata()
            .map(
                data -> {
                  try {
                    return ExporterOuterClass.ExporterAcknowledgment.parseFrom(data);
                  } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .orElse(ExporterOuterClass.ExporterAcknowledgment.newBuilder().build());

    client.open(
        lastAck,
        new StreamObserver<ExporterOuterClass.OpenResponse>() {
          @Override
          public void onNext(ExporterOuterClass.OpenResponse value) {}

          @Override
          public void onError(Throwable t) {}

          @Override
          public void onCompleted() {}
        });
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

  public long getPosition() {
    return lastAck.getPosition();
  }

  public ExporterOuterClass.ExporterAcknowledgment getLastAck() {
    return lastAck;
  }

  private final class ResponseObserver
      implements StreamObserver<ExporterOuterClass.ExporterAcknowledgment> {
    @Override
    public void onNext(ExporterOuterClass.ExporterAcknowledgment value) {
      lastAck = value;
      controller.updateLastExportedRecordPosition(value.getPosition(), value.toByteArray());
    }

    @Override
    public void onError(Throwable t) {}

    @Override
    public void onCompleted() {}
  }
}
