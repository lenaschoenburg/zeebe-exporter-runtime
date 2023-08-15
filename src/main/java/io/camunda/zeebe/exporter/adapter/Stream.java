package io.camunda.zeebe.exporter.adapter;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.camunda.zeebe.exporter.ExporterGrpc;
import io.camunda.zeebe.exporter.ExporterOuterClass;
import io.camunda.zeebe.exporter.ExporterOuterClass.ExporterAcknowledgment;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Stream {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcExporter.class);
  private final ManagedChannel channel;
  private final Controller controller;
  private final Buffer buffer = new Buffer();
  private StreamObserver<ExporterOuterClass.Record> stream;

  public Stream(final ManagedChannel channel, final Controller controller) {
    this.channel = channel;
    this.controller = controller;
  }

  void export(final Record<?> record) {
    if (stream == null) {
      stream = initializeStream();
      buffer.drain(this::sendRecord);
    }

    buffer.put(record);
    sendRecord(record);
  }

  private static ExporterAcknowledgment readAck(final Controller controller) {
    return controller
        .readMetadata()
        .map(
            data -> {
              try {
                return ExporterAcknowledgment.parseFrom(data);
              } catch (final InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
            })
        .orElseGet(ExporterAcknowledgment::getDefaultInstance);
  }

  private StreamObserver<ExporterOuterClass.Record> initializeStream() {
    //noinspection ResultOfMethodCallIgnored
    ExporterGrpc.newBlockingStub(channel).open(readAck(controller));
    return ExporterGrpc.newStub(channel).exportStream(new AckReceiver());
  }

  private void sendRecord(final Record<?> record) {
    stream.onNext(
        ExporterOuterClass.Record.newBuilder()
            .setSerialized(ByteString.copyFromUtf8(record.toJson()))
            .build());
  }

  private final class AckReceiver implements StreamObserver<ExporterAcknowledgment> {

    @Override
    public void onNext(final ExporterAcknowledgment newAck) {
      LOG.trace("Received ack {}", newAck);
      controller.updateLastExportedRecordPosition(newAck.getPosition(), newAck.toByteArray());
      buffer.ack(newAck.getPosition());
    }

    @Override
    public void onError(final Throwable t) {
      LOG.error("Stream error", t);
      stream = null;
    }

    @Override
    public void onCompleted() {
      LOG.info("Stream completed");
      stream = null;
    }
  }
}
