package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.adapter.Adapter;
import io.camunda.zeebe.exporter.runtime.ExporterDescriptor;
import io.camunda.zeebe.exporter.runtime.ExporterService;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.camunda.community.eze.EngineFactory;
import org.camunda.community.eze.ZeebeEngine;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
public class ElasticExporterTest {

  private ZeebeEngine engine;
  private Server server;
  private ManagedChannel channel;
  private Adapter adapter;

  @Container
  ElasticsearchContainer container =
      new ElasticsearchContainer(
          DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch")
              .withTag("7.16.2"));

  private RestClient client;

  @BeforeEach
  void setup() throws IOException {

    // elastic
    client =
        RestClient.builder(HttpHost.create(container.getHttpHostAddress()))
            .build();
    Response response = client.performRequest(new Request("GET", "/_cluster/health"));
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(200);

    // exporter service
    final var serverName = InProcessServerBuilder.generateName();

    server =
        InProcessServerBuilder.forName(serverName)
            .addService(
                new ExporterService(
                    List.of(
                        new ExporterDescriptor(
                            "elasticsearch",
                            ElasticsearchExporter.class,
                            Map.of("url", container.getHttpHostAddress())))))
            .build()
            .start();
    channel = InProcessChannelBuilder.forName(serverName).build();

    adapter = new Adapter(channel);
    engine = EngineFactory.INSTANCE.create(List.of(adapter));
    engine.start();
  }

  @AfterEach
  void teardown() throws InterruptedException {
    engine.stop();
    channel.shutdownNow().awaitTermination(30, TimeUnit.SECONDS);
    server.shutdown().awaitTermination(30, TimeUnit.SECONDS);
  }

  @Test
  void testExporterAdapter() {
    // given

    // when
    try (final var client = engine.createClient()) {
      client.newDeployResourceCommand()
              .addProcessModel(
                      Bpmn.createExecutableProcess()
                              .startEvent()
                              .endEvent()
                              .done(), "simple.bpmn")
              .send().join();
    }

    // then
    Awaitility.await("Indices exist").ignoreExceptions().until(() -> {
              Response response = client.performRequest(new Request("GET", "/_cat/indices"));
              return new String(response.getEntity().getContent().readAllBytes());
    }, Matchers.containsString("zeebe-record_process_"));
  }
}
