package io.zeroshift.order.web;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.zeroshift.order.infrastructure.SagaPressure;
import io.zeroshift.platform.web.ApiErrors;
import io.zeroshift.platform.web.ApiException;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

class EdgeGuardsTest {
  private final SagaPressure sagas = mock(SagaPressure.class);
  private final EdgeGuards guards = new EdgeGuards(sagas, new SimpleMeterRegistry());

  EdgeGuardsTest() {
    when(sagas.active()).thenReturn(new SagaPressure.Active(0, Instant.now(), null));
  }

  @Test
  void admitsEverythingWhileOff() throws Exception {
    for (int i = 0; i < 50; i++) assertThat(guards.admit(() -> "ok")).isEqualTo("ok");
    assertThat(guards.state().counters().admitted()).isEqualTo(50);
  }

  @Test
  void theRateLimiterRefusesWith429AndRetryAfter() throws Exception {
    guards.configure(new EdgeGuards.Settings(true, 3, false, 200, false, 6));
    for (int i = 0; i < 3; i++) guards.admit(() -> "ok");
    assertThatThrownBy(() -> guards.admit(() -> "ok"))
        .isInstanceOfSatisfying(
            ApiException.class,
            e -> {
              assertThat(e.status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
              assertThat(e.code()).isEqualTo(EdgeGuards.RATE_LIMITED);
              assertThat(e.context()).containsEntry(ApiErrors.RETRY_AFTER_SECONDS, 1);
            });
    assertThat(guards.state().counters().rateLimited()).isEqualTo(1);
  }

  @Test
  void theShedderRefusesWhileTooManyOrdersAreUnfinished() throws Exception {
    guards.configure(new EdgeGuards.Settings(false, 20, true, 10, false, 6));
    when(sagas.active()).thenReturn(new SagaPressure.Active(11, Instant.now(), null));
    assertThatThrownBy(() -> guards.admit(() -> "ok"))
        .isInstanceOfSatisfying(
            ApiException.class, e -> assertThat(e.code()).isEqualTo(EdgeGuards.LOAD_SHED));
    when(sagas.active()).thenReturn(new SagaPressure.Active(10, Instant.now(), null));
    assertThat(guards.admit(() -> "ok")).isEqualTo("ok");
    assertThat(guards.state().counters().shed()).isEqualTo(1);
  }

  @Test
  void theBulkheadRefusesAPlacementBeyondTheConcurrentLimitAndFreesItsPermit() throws Exception {
    guards.configure(new EdgeGuards.Settings(false, 20, false, 200, true, 2));
    var holding = new CountDownLatch(2);
    var release = new CountDownLatch(1);
    try (var pool = Executors.newVirtualThreadPerTaskExecutor()) {
      for (int i = 0; i < 2; i++)
        pool.submit(
            () ->
                guards.admit(
                    () -> {
                      holding.countDown();
                      return release.await(5, TimeUnit.SECONDS);
                    }));
      assertThat(holding.await(5, TimeUnit.SECONDS)).isTrue();
      assertThat(guards.state().inFlight()).isEqualTo(2);
      assertThatThrownBy(() -> guards.admit(() -> "third"))
          .isInstanceOfSatisfying(
              ApiException.class,
              e -> {
                assertThat(e.status()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                assertThat(e.code()).isEqualTo(EdgeGuards.BULKHEAD_FULL);
              });
      release.countDown();
    }
    assertThat(guards.admit(() -> "after")).isEqualTo("after");
    assertThat(guards.state().counters().bulkheadFull()).isEqualTo(1);
    assertThat(guards.state().inFlight()).isZero();
  }

  @Test
  void aFailedPlacementStillReleasesItsBulkheadPermit() {
    guards.configure(new EdgeGuards.Settings(false, 20, false, 200, true, 1));
    for (int i = 0; i < 3; i++)
      assertThatThrownBy(
              () ->
                  guards.admit(
                      () -> {
                        throw new IllegalStateException("database down");
                      }))
          .isInstanceOf(IllegalStateException.class);
    assertThat(guards.state().inFlight()).isZero();
  }

  @Test
  void rejectsLimitsBelowOne() {
    assertThatThrownBy(() -> new EdgeGuards.Settings(true, 0, false, 1, false, 1))
        .isInstanceOf(ApiException.class);
  }
}
