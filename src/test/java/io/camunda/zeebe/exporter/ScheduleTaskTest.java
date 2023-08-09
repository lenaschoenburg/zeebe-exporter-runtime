package io.camunda.zeebe.exporter;

import io.camunda.zeebe.exporter.api.Exporter;
import io.camunda.zeebe.exporter.api.context.Controller;
import io.camunda.zeebe.exporter.runtime.ExporterService;
import io.camunda.zeebe.protocol.record.Record;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class ScheduleTaskTest {

    private TestExporter testExporter;

    static final class TestExporter implements Exporter {
        public final List<Record<?>> records = new ArrayList<>();
        private Controller controller;

        @Override
        public void open(Controller controller) {
            this.controller = controller;
        }

        @Override
        public void export(Record<?> record) {
            records.add(record);
        }

        public void scheduleRunnable(Runnable run) {
            controller.scheduleCancellableTask(Duration.ZERO, run);
        }
    }

    @Test
    void shouldScheduleRunnable() throws InterruptedException {
        // given
        testExporter = new TestExporter();
        final var exporterService = new ExporterService(List.of(testExporter));
        final var countDownLatch = new CountDownLatch(1);
        exporterService.open(ExporterOuterClass.ExporterAcknowledgment.newBuilder().build(), getEmptyResponseObserver());

        // when
        testExporter.scheduleRunnable(countDownLatch::countDown);

        // then
        assertThat(countDownLatch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    @NotNull
    private static ClientResponseObserver<Object, ExporterOuterClass.OpenResponse> getEmptyResponseObserver() {
        return new ClientResponseObserver<>() {
            @Override
            public void onNext(ExporterOuterClass.OpenResponse openResponse) {

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onCompleted() {

            }

            @Override
            public void beforeStart(ClientCallStreamObserver<Object> clientCallStreamObserver) {

            }
        };
    }
}
