package io.camunda.zeebe.exporter;

import static org.assertj.core.api.Assertions.assertThat;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.zeebe.containers.ZeebeContainer;
import java.io.IOException;
import java.nio.file.Path;
import org.apache.http.HttpHost;
import org.awaitility.Awaitility;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

@Testcontainers
public class ZeebeIntegrationTest {
  private static final Network network = Network.newNetwork();
  private static final Logger LOG = LoggerFactory.getLogger(ZeebeIntegrationTest.class);

  @Container
  final ElasticsearchContainer elasticSearch =
      new ElasticsearchContainer()
          .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("ELASTICSEARCH"))
          .withNetworkAliases("elasticsearch")
          .withNetwork(network);

  @Container
  final GenericContainer exporterRuntime =
      new GenericContainer(
              DockerImageName.parse("ghcr.io/oleschoenburg/zeebe-exporter-runtime:1.0-SNAPSHOT"))
          .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("RUNTIME"))
          .withNetworkAliases("runtime")
          .withNetwork(network)
          .withEnv(
              "ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_CLASSNAME",
              "io.camunda.zeebe.exporter.ElasticsearchExporter")
          .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_URL", "http://elasticsearch:9200")
          .withEnv("ZEEBE_BROKER_EXPORTERS_ELASTICSEARCH_ARGS_BULKSIZE", "1");

  @Container
  final ZeebeContainer zeebe =
      new ZeebeContainer()
          .withLogConsumer(new Slf4jLogConsumer(LOG).withPrefix("ZEEBE"))
          .withNetwork(network)
          .withNetworkAliases("zeebe")
          .withCopyToContainer(
              MountableFile.forHostPath(
                  Path.of("target/zeebe-exporter-runtime-1.0-SNAPSHOT.jar")
                      .toAbsolutePath()
                      .toString()),
              "/usr/local/zeebe/exporters/adapter.jar")
          .withEnv(
              "ZEEBE_BROKER_EXPORTERS_ADAPTER_CLASSNAME",
              "io.camunda.zeebe.exporter.adapter.Adapter")
          .withEnv(
              "ZEEBE_BROKER_EXPORTERS_ADAPTER_JARPATH", "/usr/local/zeebe/exporters/adapter.jar")
          .withEnv("ZEEBE_BROKER_EXPORTERS_ADAPTER_ARGS_TARGET", "runtime:8080");

  @Test
  void shouldStartZeebe() throws IOException {
    // given
    final var elasticClient =
        RestClient.builder(HttpHost.create(elasticSearch.getHttpHostAddress())).build();
    Response healthResponse = elasticClient.performRequest(new Request("GET", "/_cluster/health"));
    assertThat(healthResponse.getStatusLine().getStatusCode()).isEqualTo(200);

    // when
    try (var client =
        ZeebeClient.newClientBuilder()
            .usePlaintext()
            .gatewayAddress(zeebe.getExternalGatewayAddress())
            .usePlaintext()
            .build()) {
      client
          .newDeployResourceCommand()
          .addProcessModel(
              Bpmn.createExecutableProcess().startEvent().endEvent().done(), "simple.bpmn")
          .send()
          .join();
    }

    // then
    Awaitility.await("Indices exist")
        .ignoreExceptions()
        .until(
            () -> {
              Response response = elasticClient.performRequest(new Request("GET", "/_cat/indices"));
              return new String(response.getEntity().getContent().readAllBytes());
            },
            Matchers.containsString("zeebe-record_process_"));
  }
}
