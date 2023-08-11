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
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Adapter implements Exporter {
  private static final Logger LOG = LoggerFactory.getLogger(Adapter.class);
  private ManagedChannel channel;
  private StreamObserver<ExporterOuterClass.Record> exportStream;
  private ResponseObserver responses;
  private Controller controller;
  private ExporterOuterClass.ExporterAcknowledgment lastAck;
  private Context context;
  private boolean isOpened = false;
  private long exportCounter = 0;
  private ExporterGrpc.ExporterBlockingStub blockingClient;
  private ExporterGrpc.ExporterStub asyncClient;

  public Adapter(final ManagedChannel channel) {
    this.channel = channel;
  }

  public Adapter() {}

  @Override
  public void configure(final Context context) {
    LOG.info("Configuring adapter: {}", context.getConfiguration());
    this.context = context;
  }

  @Override
  public void close() {
    channel.shutdown();
    exportStream.onCompleted();
    responses.onCompleted();
  }

  @Override
  public void open(final Controller controller) {
    if (channel == null) {
      final var arguments = context.getConfiguration().getArguments();
      final String target = arguments.get("target").toString();
      LOG.info("Open channel for target '{}'", target);
      channel = ManagedChannelBuilder.forTarget(target).usePlaintext().build();
    }
    blockingClient =
        ExporterGrpc.newBlockingStub(channel).withDeadlineAfter(10000, TimeUnit.MILLISECONDS);
    asyncClient = ExporterGrpc.newStub(channel).withDeadlineAfter(10000, TimeUnit.MILLISECONDS);

    this.controller = controller;
    responses = new ResponseObserver();
    lastAck =
        controller
            .readMetadata()
            .map(
                data -> {
                  try {
                    return ExporterOuterClass.ExporterAcknowledgment.parseFrom(data);
                  } catch (final InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                  }
                })
            .orElse(ExporterOuterClass.ExporterAcknowledgment.newBuilder().build());

    LOG.info("Read lastAck '{}'", lastAck);
  }

  @Override
  public void export(final Record<?> record) {
    if (!isOpened) {
      final var response = blockingClient.withWaitForReady().open(lastAck);
      isOpened = true;
      LOG.info("Opened stream");
      exportStream = asyncClient.exportStream(new ResponseObserver());
    }

    final var r =
        ExporterOuterClass.Record.newBuilder()
            .setSerialized(ByteString.copyFromUtf8(record.toJson()))
            .build();

    if (exportCounter++ % 1000 == 0) {
      LOG.info("Exported {} record", exportCounter);
    }
    exportStream.onNext(r);
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
    public void onNext(final ExporterOuterClass.ExporterAcknowledgment value) {
      LOG.info("Received ack '{}' from exporter stream", value);
      lastAck = value;
      controller.updateLastExportedRecordPosition(value.getPosition(), value.toByteArray());
    }

    @Override
    public void onError(final Throwable t) {
      LOG.error("Failed stream", t);
    }

    @Override
    public void onCompleted() {
      LOG.info("Completed stream");
    }
  }
}
