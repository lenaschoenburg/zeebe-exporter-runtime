package io.camunda.zeebe.exporter.adapter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Configuration;
import io.camunda.zeebe.exporter.api.context.Context;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class GrpcExporter implements Exporter {
  private static final Logger LOG = LoggerFactory.getLogger(GrpcExporter.class);
  private ManagedChannel channel;
  private Stream stream;
  private Configuration configuration;

  public GrpcExporter(final ManagedChannel channel) {
    this.channel = channel;
  }

  @SuppressWarnings("unused")
  public GrpcExporter() {}

  @Override
  public void configure(final Context context) {
    configuration = context.getConfiguration();
  }

  @Override
  public void close() {
    if (channel != null) {
      channel.shutdown();
    }
    channel = null;
    stream = null;
  }

  @Override
  public void open(final Controller controller) {
    if (channel == null) {
      channel = buildChannel(configuration);
    }
    stream = new Stream(channel, controller);
  }

  private static ManagedChannel buildChannel(final Configuration configuration) {
    final var arguments = configuration.getArguments();
    final var target = arguments.get("target").toString();
    LOG.info("Open channel for target '{}'", target);
    return ManagedChannelBuilder.forTarget(target).usePlaintext().build();
  }

  @Override
  public void export(final Record<?> record) {
    stream.export(record);
  }
}
