package io.zeroshift.failures;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import tools.jackson.databind.node.ObjectNode;

/**
 * A {@link Coordinator} running as a child JVM. The control plane reads its JSON lines, waits for
 * the event it expects, and can kill it with SIGKILL ({@link Process#destroyForcibly()}) at a pause
 * point. Nothing about the crash is simulated: the process, its heap and its PostgreSQL sessions
 * are gone, and what survives is only what the databases hold.
 */
final class CoordinatorProcess implements AutoCloseable {
  private static final JsonMapper JSON = JsonMapper.builder().build();

  private final Process process;
  private final Writer stdin;
  private final List<ObjectNode> events = Collections.synchronizedList(new ArrayList<>());
  private final Instant started = Instant.now();
  private final String mode;

  private CoordinatorProcess(String mode, Process process) {
    this.mode = mode;
    this.process = process;
    this.stdin = new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8);
    Thread.ofVirtual()
        .name("coordinator-" + process.pid())
        .start(
            () -> {
              try (var out =
                  new BufferedReader(
                      new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                for (String line; (line = out.readLine()) != null; ) {
                  if (!line.startsWith("{")) continue;
                  try {
                    var node = (ObjectNode) JSON.readTree(line);
                    events.add(node);
                    synchronized (events) {
                      events.notifyAll();
                    }
                  } catch (RuntimeException ignored) {
                    // Not one of the coordinator's lines (a JVM warning, say).
                  }
                }
              } catch (IOException ignored) {
                // The process ended.
              }
              synchronized (events) {
                events.notifyAll();
              }
            });
  }

  /** Starts a coordinator with {@code key=value} arguments; the DB password goes via env. */
  static CoordinatorProcess start(String mode, Map<String, String> args, String dbPassword)
      throws IOException {
    var command = new ArrayList<String>();
    command.add(ProcessHandle.current().info().command().orElse("java"));
    // A small, quick JVM: it runs for a second and does plain JDBC.
    command.addAll(List.of("-Xmx64m", "-XX:+UseSerialGC", "-XX:TieredStopAtLevel=1", "-Xss512k"));
    String classpath = System.getProperty("java.class.path");
    command.add("-cp");
    command.add(classpath);
    if (isBootJar(classpath)) {
      // The packaged app is a Spring Boot jar: its classes and libraries are nested, so launch
      // the coordinator through Boot's PropertiesLauncher with the coordinator as main class.
      command.add("-Dloader.main=" + Coordinator.class.getName());
      command.add("org.springframework.boot.loader.launch.PropertiesLauncher");
    } else {
      command.add(Coordinator.class.getName());
    }
    command.add("mode=" + mode);
    args.forEach((k, v) -> command.add(k + "=" + v));
    var builder = new ProcessBuilder(command).redirectErrorStream(true);
    // The control plane's OpenTelemetry agent must not attach to the child.
    builder.environment().remove("JAVA_TOOL_OPTIONS");
    builder.environment().put("FL_DB_PASSWORD", dbPassword == null ? "" : dbPassword);
    return new CoordinatorProcess(mode, builder.start());
  }

  static boolean isBootJar(String classpath) {
    if (classpath.contains(File.pathSeparator) || !classpath.endsWith(".jar")) return false;
    try (var jar = new JarFile(classpath)) {
      return jar.getEntry("BOOT-INF/") != null || jar.getEntry("BOOT-INF/classes/") != null;
    } catch (IOException e) {
      return false;
    }
  }

  long pid() {
    return process.pid();
  }

  String mode() {
    return mode;
  }

  Instant started() {
    return started;
  }

  boolean alive() {
    return process.isAlive();
  }

  List<ObjectNode> events() {
    synchronized (events) {
      return List.copyOf(events);
    }
  }

  /** Waits for an event of this type; fails with what the process said if it ends first. */
  JsonNode await(String type, Duration timeout) throws InterruptedException {
    long deadline = System.nanoTime() + timeout.toNanos();
    synchronized (events) {
      while (true) {
        for (var e : events) if (e.path("event").asString().equals(type)) return e;
        for (var e : events)
          if (e.path("event").asString().equals("ERROR"))
            throw new IllegalStateException("Coordinator failed: " + e.path("error").asString());
        if (!process.isAlive()
            && events.stream().noneMatch(e -> e.path("event").asString().equals(type))) {
          // Give the reader thread a moment to drain the last lines.
          events.wait(200);
          for (var e : events) if (e.path("event").asString().equals(type)) return e;
          throw new IllegalStateException(
              "Coordinator exited (" + process.exitValue() + ") before " + type + ": " + events);
        }
        long left = deadline - System.nanoTime();
        if (left <= 0)
          throw new IllegalStateException("No " + type + " from the coordinator within " + timeout);
        events.wait(Math.max(1, TimeUnit.NANOSECONDS.toMillis(left)));
      }
    }
  }

  /** SIGKILL, then the exit status the operating system reports (137 = 128 + SIGKILL). */
  int kill() throws InterruptedException {
    process.destroyForcibly();
    if (!process.waitFor(10, TimeUnit.SECONDS))
      throw new IllegalStateException("The coordinator did not die within 10 s");
    return process.exitValue();
  }

  /** Waits for a normal exit and returns its code. */
  int awaitExit(Duration timeout) throws InterruptedException {
    if (!process.waitFor(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
      process.destroyForcibly();
      throw new IllegalStateException("The coordinator did not finish within " + timeout);
    }
    return process.exitValue();
  }

  void resume() throws IOException {
    stdin.write("continue\n");
    stdin.flush();
  }

  @Override
  public void close() {
    if (process.isAlive()) process.destroyForcibly();
  }
}
