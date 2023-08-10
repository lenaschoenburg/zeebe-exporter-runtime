package io.camunda.zeebe.exporter;

import static io.camunda.zeebe.exporter.runtime.Runtime.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

public class ConfigurationTest {
  @Test
  void shouldReadConfig() {
    // given
    final var env = Map.of("ZEEBE_BROKER_EXPORTERS_TESTEXPORTER_SOMETHING_TEST", "1234");

    // when
    final var config = parseConfiguration(env);

    // then
    assertThat(config).containsEntry("testexporter", Map.of("something", Map.of("test", "1234")));
  }

  @Test
  void shouldReadMultipleEnvVars() {
    // given
    final var env = Map.of("ZEEBE_BROKER_EXPORTERS_TESTEXPORTER_SOMETHING_TEST", "1234", "ZEEBE_BROKER_EXPORTERS_TESTEXPORTER_OTHER_TEST", "5678");

    // when
    final var config = parseConfiguration(env);

    // then
    assertThat(config).containsEntry("testexporter", Map.of("something", Map.of("test", "1234"), "other", Map.of("test", "5678")));
  }
}
