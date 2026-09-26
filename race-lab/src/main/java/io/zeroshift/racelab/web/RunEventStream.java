package io.zeroshift.racelab.web;

import io.zeroshift.racelab.application.RunStreams;
import io.zeroshift.racelab.application.port.RunRepository;
import io.zeroshift.racelab.domain.RaceEvent;
import io.zeroshift.racelab.domain.RaceLabErrors;
import io.zeroshift.racelab.domain.Run;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.http.MediaType;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * A run's events as server-sent events: everything recorded so far, then each new event as the
 * engine records it, then the finished run. Events carry their sequence number; the browser orders
 * by it and ignores repeats.
 */
public class RunEventStream {
  private static final long TIMEOUT_MILLIS = 10 * 60 * 1000;

  private final RunStreams streams;
  private final RunRepository runs;

  public RunEventStream(RunStreams streams, RunRepository runs) {
    this.streams = streams;
    this.runs = runs;
  }

  public SseEmitter open(long runId, int afterSeq) {
    var run = runs.find(runId).orElseThrow(() -> new RaceLabErrors.RunNotFound(runId));
    var emitter = new SseEmitter(TIMEOUT_MILLIS);
    var sent = ConcurrentHashMap.<Integer>newKeySet();
    var lock = new Object();
    var buffered = new ArrayList<RaceEvent>();
    var replaying = new boolean[] {true};
    var closed = new boolean[] {false};

    Runnable unsubscribe =
        streams.subscribe(
            runId,
            new RunStreams.Listener() {
              @Override
              public void event(RaceEvent event) {
                synchronized (lock) {
                  if (replaying[0]) buffered.add(event);
                  else send(emitter, event, sent, closed);
                }
              }

              @Override
              public void finished(Run finished) {
                synchronized (lock) {
                  if (replaying[0]) return; // the replay below reads the final state itself
                  finish(emitter, finished, closed);
                }
              }
            });
    emitter.onCompletion(unsubscribe);
    emitter.onTimeout(unsubscribe);
    emitter.onError(e -> unsubscribe.run());

    synchronized (lock) {
      List<RaceEvent> recorded =
          streams.recorded(runId).orElseGet(() -> runs.events(runId, afterSeq, 100_000));
      for (var e : recorded) if (e.seq() > afterSeq) send(emitter, e, sent, closed);
      for (var e : buffered) send(emitter, e, sent, closed);
      buffered.clear();
      replaying[0] = false;
      var now = runs.find(runId).orElse(run);
      if (now.status() == Run.Status.COMPLETED || now.status() == Run.Status.FAILED) {
        // Finished before or during the replay: flush whatever was persisted meanwhile, then end.
        for (var e : runs.events(runId, afterSeq, 100_000)) send(emitter, e, sent, closed);
        finish(emitter, now, closed);
      } else {
        sendRun(emitter, now, closed);
      }
    }
    return emitter;
  }

  private static void send(SseEmitter emitter, RaceEvent e, Set<Integer> sent, boolean[] closed) {
    if (closed[0] || !sent.add(e.seq())) return;
    try {
      emitter.send(
          SseEmitter.event().name("event").id(String.valueOf(e.seq())).data(e, MediaType.APPLICATION_JSON));
    } catch (IOException | IllegalStateException ex) {
      closed[0] = true;
    }
  }

  private static void sendRun(SseEmitter emitter, Run run, boolean[] closed) {
    if (closed[0]) return;
    try {
      emitter.send(SseEmitter.event().name("run").data(run, MediaType.APPLICATION_JSON));
    } catch (IOException | IllegalStateException ex) {
      closed[0] = true;
    }
  }

  private static void finish(SseEmitter emitter, Run run, boolean[] closed) {
    sendRun(emitter, run, closed);
    if (!closed[0]) {
      closed[0] = true;
      emitter.complete();
    }
  }
}
